use dashmap::DashMap;
use futures::FutureExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, trace};

use crate::common::traits::SchedulableTask;
use crate::common::{
    Extensions, SchedulerError, TaskContext, TaskData, TaskState, TimeUtils, calculate_backoff,
};
use crate::driver::PacemakerEvent;
use crate::driver::context::DriverContext;
use crate::driver::pacemaker::TaskPacemaker;
use crate::driver::plugin::DriverPlugin;
use crate::persistence::{AcquireItem, LoadStatus, TaskStore};
use crate::policy::WaitStrategy;

// =========================================================================
// 1. 基础数据结构
// =========================================================================

/// 运行中的任务句柄
struct RunningTaskHandle {
    epoch: u64,
    token: CancellationToken,
}

/// 许可守卫 (RAII) - Drop 时自动归还信号量
struct PermitGuard {
    semaphore: Arc<Semaphore>,
    permits: u32,
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.semaphore.add_permits(self.permits as usize);
    }
}

// =========================================================================
// 2. 共享状态 (Shared State)
// =========================================================================

/// 驱动器共享状态容器
struct DriverSharedState<T> {
    ctx: DriverContext<T>,
    registry: DashMap<String, RunningTaskHandle>,
    semaphore: Arc<Semaphore>,
    plugins: Vec<Box<dyn DriverPlugin<T>>>,
    wait_strategy: Arc<dyn WaitStrategy>,
    extensions: Arc<Extensions>,
    paused: AtomicBool,
    notify: Notify,
}

impl<T> DriverSharedState<T> {
    fn new(
        ctx: DriverContext<T>,
        plugins: Vec<Box<dyn DriverPlugin<T>>>,
        wait_strategy: Arc<dyn WaitStrategy>,
        extensions: Extensions,
    ) -> Self {
        let concurrency = ctx.config.worker.max_concurrency;
        Self {
            ctx,
            registry: DashMap::new(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            plugins,
            wait_strategy,
            extensions: Arc::new(extensions),
            paused: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn is_shutdown(&self) -> bool {
        self.ctx.is_shutdown()
    }
}

// =========================================================================
// 3. 执行器 (Executor)
// =========================================================================

/// 专注负责任务的执行生命周期
struct TaskExecutor<Task, T> {
    state: Arc<DriverSharedState<T>>,
    handler: Arc<Task>,
}

impl<Task, T> TaskExecutor<Task, T>
where
    Task: SchedulableTask<T> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    /// 执行管线：Load -> Hooks -> Execute -> Ack/Fail -> Hooks
    #[instrument(
        name = "exec_task",
        skip(self, token),
        fields(task_id = %task_id, epoch = %epoch)
    )]
    async fn process_pipeline(&self, task_id: String, epoch: u64, token: CancellationToken) {
        // 1. Load Data
        let task_data = match self.load_task_data(&task_id).await {
            Some(data) => data,
            None => {
                trace!("Task data load failed or corrupted, skipping execution");
                return;
            } // 数据已损坏或丢失，跳过
        };

        // 2. Prepare Context
        let runtime_ctx = TaskContext::new(
            task_data.clone(),
            token.clone(),
            self.state.extensions.clone(),
        );

        let mut task_data = task_data;
        task_data.mark_running(self.state.ctx.node_id.clone(), epoch);

        // 3. Before Hook
        for p in self.state.plugins.iter() {
            p.before_execute(&runtime_ctx).await;
        }

        // 4. Execute (Panic Safe)
        let start_time = TimeUtils::now();
        let result = AssertUnwindSafe(self.handler.execute(runtime_ctx.clone()))
            .catch_unwind()
            .await;
        let duration = TimeUtils::now() - start_time;

        // 5. After Hook
        for p in self.state.plugins.iter() {
            p.after_execute(&runtime_ctx, duration).await;
        }

        // 6. Handle Result
        match result {
            Ok(Ok(_)) => {
                for p in self.state.plugins.iter() {
                    p.on_success(&runtime_ctx).await;
                }
                self.finalize_success(task_data).await;
            }
            Ok(Err(e)) => {
                let err_msg = e.to_string();
                for p in self.state.plugins.iter() {
                    p.on_failure(&runtime_ctx, &err_msg).await;
                }
                self.finalize_failure(task_data, err_msg).await;
            }
            Err(panic_err) => {
                let msg = Self::format_panic(panic_err);
                error!("[Executor] Task {} panicked: {}", task_id, msg);
                for p in self.state.plugins.iter() {
                    p.on_failure(&runtime_ctx, &msg).await;
                }
                self.finalize_failure(task_data, msg).await;
            }
        }
        trace!("Task execution pipeline finished");
        // 7. Cleanup Registry
        self.state.registry.remove(&task_id);
    }

    /// 辅助：加载任务数据
    #[instrument(skip(self), level = "trace")]
    async fn load_task_data(&self, task_id: &str) -> Option<TaskData<T>> {
        let store = &self.state.ctx.store;
        match store.load(task_id).await {
            Ok(LoadStatus::Found(data)) => Some(data),
            Ok(LoadStatus::DataCorrupted { .. }) | Ok(LoadStatus::NotFound) => {
                // 脏数据自动清理
                let _ = store.remove(task_id).await;
                self.state.registry.remove(task_id);
                None
            }
            Err(e) => {
                error!(error = ?e, "Load task failed");
                self.state.registry.remove(task_id);
                None
            }
        }
    }

    /// 辅助：处理成功
    #[instrument(skip(self, task), fields(task_id = %task.id), level = "debug")]
    async fn finalize_success(&self, mut task: TaskData<T>) {
        trace!("Handling task success");
        let queue = &self.state.ctx.queue;
        let store = &self.state.ctx.store;

        // 释放运行锁
        let _ = queue
            .release(&[task.id.clone()], &self.state.ctx.node_id)
            .await;

        let now = TimeUtils::now_f64();
        // 计算下次时间（如果是周期任务）
        let next_time =
            TimeUtils::next_recurrence_time(&task.schedule_type, task.timezone.as_deref(), now);

        if let Some(ts) = next_time {
            task.next_poll_at = ts;
            task.state = TaskState::Pending;
            task.epoch = 0;
            task.worker_id = None;
            task.updated_at = now;
            if let Err(e) = store.requeue(task).await {
                error!("[Executor] Requeue failed: {:?}", e);
            }
        } else {
            if let Err(e) = store.remove(&task.id).await {
                error!("[Executor] Remove failed: {:?}", e);
            }
        }
    }

    /// 辅助：处理失败
    #[instrument(skip(self, task), fields(task_id = %task.id), level = "warn")]
    async fn finalize_failure(&self, mut task: TaskData<T>, err_msg: String) {
        let queue = &self.state.ctx.queue;
        let store = &self.state.ctx.store;
        error!(error = %err_msg, "Task execution failed");
        let _ = queue
            .release(&[task.id.clone()], &self.state.ctx.node_id)
            .await;

        if task.can_retry() {
            task.last_error = Some(err_msg);
            let backoff = calculate_backoff(task.attempt, 1.0, 3600.0);
            task.next_poll_at = TimeUtils::now() + backoff.as_secs_f64();
            task.state = TaskState::Pending;
            task.epoch = 0;
            task.worker_id = None;
            task.updated_at = TimeUtils::now();

            if let Err(e) = store.requeue(task.clone()).await {
                error!("[Executor] Retry failed: {:?}", e);
            }
        } else {
            if let Err(e) = store.move_to_dlq(&task, err_msg).await {
                error!("[Executor] DLQ move failed: {:?}", e);
            }
        }
    }

    fn format_panic(err: Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = err.downcast_ref::<&str>() {
            format!("Panic: {}", s)
        } else if let Some(s) = err.downcast_ref::<String>() {
            format!("Panic: {}", s)
        } else {
            "Panic: Unknown error".to_string()
        }
    }
}

// =========================================================================
// 4. 消费者 (Consumer)
// =========================================================================

/// 任务消费者
///
/// 职责：
/// 1. 运行起搏器 (Pacemaker)
/// 2. 申请信号量
/// 3. 从队列拉取任务
/// 4. 派发给 Executor
struct TaskConsumer<Task, T> {
    state: Arc<DriverSharedState<T>>,
    executor: Arc<TaskExecutor<Task, T>>,
}

impl<Task, T> TaskConsumer<Task, T>
where
    Task: SchedulableTask<T> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    #[instrument(name = "consumer", skip_all, fields(node_id = %self.state.ctx.node_id))]
    async fn run_loop(&self) {
        let mut pacemaker = TaskPacemaker::new(
            &self.state.paused,
            &self.state.notify,
            &self.state.ctx.shutdown,
            self.state.wait_strategy.clone(),
        );
        let batch_size = self.state.ctx.config.cluster.acquire_batch_size;

        loop {
            // 1. Pacemaker Wait
            match pacemaker.wait_next().await {
                PacemakerEvent::Trigger => {}
                PacemakerEvent::Shutdown => break,
                PacemakerEvent::Idle => continue,
                PacemakerEvent::Reload => {}
            }

            // 2. Plugin Flow Control
            if !self.check_plugins_allow_fetch().await {
                pacemaker.mark_idle();
                continue;
            }

            // 3. Acquire Permits (Semaphore)
            let permits = match self.acquire_permits(batch_size, &mut pacemaker).await {
                Some(p) => p,
                None => continue,
            };

            // 4. Acquire Tasks (Queue)
            let ask_size = permits.permits as usize;
            match self
                .state
                .ctx
                .queue
                .acquire(&self.state.ctx.node_id, ask_size, 3)
                .await
            {
                Ok(items) => {
                    let count = items.len();
                    if count == 0 {
                        pacemaker.mark_idle();
                        continue;
                    }

                    pacemaker.mark_busy();
                    // 优化：部分拉取时，归还多余的 Permit
                    permits.semaphore.add_permits(ask_size - count);
                    // 转移所有权：剩下的 Permit 交给 Spawn 里的 Guard 管理
                    std::mem::forget(permits);

                    // 5. Spawn
                    for item in items {
                        self.spawn_task(item).await;
                    }
                }
                Err(e) => {
                    error!("[Consumer] Acquire failed: {:?}", e);
                    pacemaker.mark_idle();
                    // permits Drops here, returning all permits to semaphore
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn check_plugins_allow_fetch(&self) -> bool {
        for plugin in self.state.plugins.iter() {
            if !plugin.before_fetch(&self.state.ctx).await {
                return false;
            }
        }
        true
    }

    async fn acquire_permits(
        &self,
        batch_size: usize,
        pacemaker: &mut TaskPacemaker<'_>,
    ) -> Option<PermitGuard> {
        let available = self.state.semaphore.available_permits();
        // 如果完全满载，挂起等待至少一个空位，而不是忙轮询
        if available == 0 {
            match tokio::time::timeout(Duration::from_secs(1), self.state.semaphore.acquire()).await
            {
                Ok(Ok(p)) => drop(p), // 等到了，立即释放，准备批量申请
                _ => {
                    pacemaker.mark_idle(); // 等太久，退避一下
                    return None;
                }
            }
        }

        let current_avail = self.state.semaphore.available_permits();
        if current_avail == 0 {
            return None;
        }

        let ask_size = batch_size.min(current_avail);
        match self
            .state
            .semaphore
            .clone()
            .acquire_many_owned(ask_size as u32)
            .await
        {
            Ok(_p) => Some(PermitGuard {
                semaphore: self.state.semaphore.clone(),
                permits: ask_size as u32,
            }),
            Err(_) => None,
        }
    }

    async fn spawn_task(&self, item: AcquireItem) {
        let executor = self.executor.clone();
        let state = self.state.clone();

        let token = CancellationToken::new();
        state.registry.insert(
            item.id.clone(),
            RunningTaskHandle {
                epoch: item.score,
                token: token.clone(),
            },
        );

        // 重新封装 PermitGuard，每个任务持有一个
        let permit = PermitGuard {
            semaphore: state.semaphore.clone(),
            permits: 1,
        };

        tokio::spawn(async move {
            let _guard = permit; // 任务结束自动归还 1 个信号量
            executor.process_pipeline(item.id, item.score, token).await;
        });
    }
}

// =========================================================================
// 5. 维护者 (Maintainer)
// =========================================================================

/// 系统维护服务
///
/// 职责：
/// 1. 发送心跳 (Heartbeat)
/// 2. 回收僵尸任务 (Rescue)
struct SystemMaintainer<T> {
    state: Arc<DriverSharedState<T>>,
}

impl<T> SystemMaintainer<T>
where
    T: Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
{
    #[instrument(name = "heartbeat", skip_all)]
    async fn start_heartbeat(&self) {
        let interval = Duration::from_millis(self.state.ctx.config.cluster.heartbeat_interval_ms);
        loop {
            sleep(interval).await;

            // 只有当停机且任务全空时，才停止心跳
            if self.state.is_shutdown() && self.state.registry.is_empty() {
                break;
            }
            // 快照当前任务列表
            let tasks: Vec<(String, u64)> = self
                .state
                .registry
                .iter()
                .map(|e| (e.key().clone(), e.value().epoch))
                .collect();

            if tasks.is_empty() {
                continue;
            }
            // 批量心跳
            for batch in tasks.chunks(100) {
                match self
                    .state
                    .ctx
                    .queue
                    .heartbeat(batch, &self.state.ctx.node_id)
                    .await
                {
                    Ok(_) => {}
                    Err(SchedulerError::FencingTokenMismatch { task_id, .. }) => {
                        error!("[Maintainer] Split Brain detected! Cancelling");
                        if let Some(handle) = self.state.registry.get(&task_id) {
                            handle.token.cancel();
                            self.state.registry.remove(&task_id); // 立即移除，防止下次心跳还发
                        }
                    }
                    Err(e) => trace!("[Maintainer] Heartbeat err: {:?}", e),
                }
            }
        }
    }
    /// 启动救援服务
    #[instrument(name = "rescue", skip_all)]
    async fn start_rescue(&self) {
        let config = &self.state.ctx.config.policy;
        if config.zombie_check_interval_ms == 0 {
            return;
        }

        let interval = Duration::from_millis(config.zombie_check_interval_ms);
        loop {
            sleep(interval).await;
            if self.state.is_shutdown() {
                break;
            }

            let now = TimeUtils::now();
            let threshold = config.zombie_threshold_ms;

            match self
                .state
                .ctx
                .queue
                .rescue(
                    &self.state.ctx.node_id,
                    now,
                    threshold,
                    config.rescue_batch_size,
                )
                .await
            {
                Ok(ids) => {
                    if !ids.is_empty() {
                        info!("[Maintainer] Rescued {} zombies.", ids.len());
                    }
                }
                Err(e) => error!("[Maintainer] Rescue failed: {:?}", e),
            }
        }
    }
}

// =========================================================================
// 6. 任务驱动器门面 (TaskDriver - Public API)
// =========================================================================

/// 任务驱动器
pub struct TaskDriver<Task, T> {
    state: Arc<DriverSharedState<T>>,
    executor: Arc<TaskExecutor<Task, T>>,
    consumer: Arc<TaskConsumer<Task, T>>,
    maintainer: Arc<SystemMaintainer<T>>,
    _marker: PhantomData<Task>,
}

impl<Task, T> Clone for TaskDriver<Task, T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            executor: self.executor.clone(),
            consumer: self.consumer.clone(),
            maintainer: self.maintainer.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Task, T> TaskDriver<Task, T>
where
    Task: SchedulableTask<T> + Send + Sync + 'static,
    T: Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
{
    /// 构造函数
    pub fn new_with_components(
        ctx: DriverContext<T>,
        task_handler: Task,
        plugins: Vec<Box<dyn DriverPlugin<T>>>,
        wait_strategy: Arc<dyn WaitStrategy>,
        extensions: Extensions,
    ) -> Self {
        // 1. 创建共享状态
        let state = Arc::new(DriverSharedState::new(
            ctx,
            plugins,
            wait_strategy,
            extensions,
        ));
        // 2. 创建执行器
        let executor = Arc::new(TaskExecutor {
            state: state.clone(),
            handler: Arc::new(task_handler),
        });
        // 3. 创建消费者
        let consumer = Arc::new(TaskConsumer {
            state: state.clone(),
            executor: executor.clone(),
        });
        // 4. 创建维护者
        let maintainer = Arc::new(SystemMaintainer {
            state: state.clone(),
        });

        Self {
            state,
            executor,
            consumer,
            maintainer,
            _marker: PhantomData,
        }
    }

    /// 对外暴露 Store (方便测试或 API 调用)
    pub fn store(&self) -> Arc<dyn TaskStore<T>> {
        self.state.ctx.store.clone()
    }

    /// 启动引擎
    #[instrument(name = "driver_start", skip(self), fields(node_id = %self.state.ctx.node_id))]
    pub async fn start(&self) {
        // [Hook] 启动
        for p in self.state.plugins.iter() {
            p.on_start(&self.state.ctx).await;
        }
        info!("Starting Talos Driver...");
        // 1. 启动后台维护任务
        self.spawn_background();
        // 2. 启动消费者 (阻塞直到 Shutdown)
        self.run_consumers().await;
        info!("Initiating graceful shutdown sequence");
        // 3. 优雅停机
        self.graceful_shutdown_sequence().await;
        // [Hook] 关闭
        for p in self.state.plugins.iter() {
            p.on_shutdown(&self.state.ctx).await;
        }
        info!("Driver shutdown complete");
    }

    /// 手动取消单个任务 (API 支持)
    pub fn cancel_task(&self, task_id: &str) -> bool {
        if let Some(handle) = self.state.registry.get(task_id) {
            handle.token.cancel();
            info!("[Driver] Manually cancelled task {}", task_id);
            return true;
        }
        false
    }

    /// 触发停机
    pub fn shutdown(&self) {
        info!("[Driver] Shutdown triggered manually.");
        self.state.ctx.shutdown.cancel();
    }

    // --- Private Helpers ---

    fn spawn_background(&self) {
        let m1 = self.maintainer.clone();
        tokio::spawn(async move {
            m1.start_heartbeat().await;
        });

        let m2 = self.maintainer.clone();
        tokio::spawn(async move {
            m2.start_rescue().await;
        });
    }

    async fn run_consumers(&self) {
        let workers = self.state.ctx.config.worker.workers.max(1);
        let mut join_set = tokio::task::JoinSet::new();

        for i in 0..workers {
            let consumer = self.consumer.clone();
            join_set.spawn(async move {
                trace!("[Driver] Starting Consumer-{}", i);
                consumer.run_loop().await;
            });
        }
        while let Some(_) = join_set.join_next().await {}
        trace!("[Driver] All consumers stopped.");
    }
    #[instrument(skip(self))]
    async fn graceful_shutdown_sequence(&self) {
        let count = self.state.registry.len();
        if count == 0 {
            return;
        }

        let timeout_sec = self.state.ctx.config.policy.shutdown_timeout_secs.max(1);
        let max_concurrency = self.state.ctx.config.worker.max_concurrency as u32;

        info!(
            "[Driver] Waiting for {} tasks... (Timeout: {}s)",
            count, timeout_sec
        );

        // 优化：利用 Semaphore 特性等待，而不是轮询
        let wait_drain = self.state.semaphore.acquire_many(max_concurrency);
        match tokio::time::timeout(Duration::from_secs(timeout_sec), wait_drain).await {
            Ok(_) => info!("[Driver] Drained successfully."),
            Err(_) => {
                let remaining = self.state.registry.len();
                error!(
                    "[Driver] Shutdown Timeout! Force killing {} tasks.",
                    remaining
                );
                self.force_cancel_all();
            }
        }
    }

    /// 强制取消所有任务
    pub fn force_cancel_all(&self) {
        let mut count = 0;
        for entry in self.state.registry.iter() {
            if !entry.value().token.is_cancelled() {
                entry.value().token.cancel();
                count += 1;
            }
        }
        info!("[Driver] Broadcast cancel signal to {} tasks.", count);
    }
}
