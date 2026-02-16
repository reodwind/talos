use dashmap::DashMap;
use futures::FutureExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace};

use crate::common::traits::SchedulableTask;
use crate::common::{TaskContext, TaskData, TaskState, TimeUtils, calculate_backoff};
use crate::driver::PacemakerEvent;
use crate::driver::context::DriverContext;
use crate::driver::pacemaker::TaskPacemaker;
use crate::driver::plugin::DriverPlugin;
use crate::persistence::{AcquireItem, LoadStatus, TaskStore};
use crate::policy::WaitStrategy;

/// 许可守卫 (PermitGuard) - 用于批量占座的 RAII 结构体
struct PermitGuard {
    semaphore: Arc<Semaphore>,
    permits: u32,
}
/// 释放许可时自动归还给信号量
impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.semaphore.add_permits(self.permits as usize);
    }
}

struct RunningTaskHandle {
    epoch: u64,
    token: CancellationToken,
}
/// 驱动器Inner 结构体
struct DriverInner<Task, T> {
    /// 全局上下文
    ctx: DriverContext<T>,
    /// 用户具体的任务实现
    task_handler: Task,
    /// 正在运行的任务注册表 (ID -> {Epoch,cancel_tx})
    running_tasks: DashMap<String, RunningTaskHandle>,
    /// 并发控制信号量
    semaphore: Arc<Semaphore>,
    /// 插件系统
    plugins: Vec<Box<dyn DriverPlugin<T>>>,
    /// 组合等待策略
    pub wait_strategy: Arc<dyn WaitStrategy>,
    /// 全局暂停开关
    pub(crate) paused: AtomicBool,
    /// 全局的 notify，Resume 时也会触发这个
    pub(crate) notify: Notify,
}
/// 任务驱动器 (The Engine)
pub struct TaskDriver<Task, T> {
    inner: Arc<DriverInner<Task, T>>, //减轻Arc Clone
    _marker: std::marker::PhantomData<T>,
}

impl<Task, T> Clone for TaskDriver<Task, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
    ) -> Self {
        let concurrency = ctx.config.worker.max_concurrency;
        // 构建 Inner
        let inner = DriverInner {
            ctx,
            task_handler,
            running_tasks: DashMap::new(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            plugins,
            wait_strategy,
            paused: AtomicBool::new(false),
            notify: Notify::new(),
        };
        Self {
            inner: Arc::new(inner),
            _marker: PhantomData,
        }
    }

    /// 暴露内部储存
    pub fn store(&self) -> Arc<dyn TaskStore<T>> {
        self.inner.ctx.store.clone()
    }

    /// 启动引擎
    pub async fn start(&self) {
        // [Hook] 启动
        for p in self.inner.plugins.iter() {
            p.on_start(&self.inner.ctx).await;
        }

        trace!(
            "[Driver-{}] Started. Waiting for tasks...",
            self.inner.ctx.node_id
        );

        // 1. 启动心跳协程
        let heartbeat_driver = self.clone();
        tokio::spawn(async move {
            heartbeat_driver.heartbeat_loop().await;
        });

        // 1.1. 启动僵尸任务回收协程 (Rescue Loop)
        // 设计原则：与 Heartbeat 保持一致，独立后台 Loop
        let rescue_driver = self.clone();
        tokio::spawn(async move {
            rescue_driver.rescue_loop().await;
        });

        // 2. 启动拉取主循环 (阻塞直到 shutdown)
        let fetcher_count = self.inner.ctx.config.worker.workers.max(1);
        trace!("[Driver] Spawning {} fetch loops.", fetcher_count);
        let mut join_set = tokio::task::JoinSet::new();
        for _i in 0..fetcher_count {
            let driver_clone = self.clone();
            join_set.spawn(async move {
                driver_clone.fetch_loop().await;
            });
        }
        // 3. 等待所有拉取协程退出
        while let Some(_) = join_set.join_next().await {}

        // [Hook] 关闭
        for p in self.inner.plugins.iter() {
            p.on_shutdown(&self.inner.ctx).await;
        }
        trace!("[Driver-{}] Shutdown complete.", self.inner.ctx.node_id);
    }

    /// 任务拉取主循环
    ///
    /// 职责：
    /// 1. 监听起搏器 (Pacemaker) 的信号 (Trigger/Shutdown)。
    /// 2. 执行插件的前置检查 (before_fetch)。
    /// 3. 申请并发许可 (Semaphore)。
    /// 4. 从持久化层批量拉取任务 (Acquire)。
    /// 5. 异步分发任务 (Spawn)。
    async fn fetch_loop(&self) {
        // [Init] 初始化起搏器
        // 它负责管理心跳节奏、监听 Pause 信号、处理 Shutdown 信号。
        let mut pacemaker = TaskPacemaker::new(
            &self.inner.paused,
            &self.inner.notify,
            &self.inner.ctx.shutdown,
            self.inner.wait_strategy.clone(),
        );
        let semaphore = &self.inner.semaphore;
        let batch_size_cfg = self.inner.ctx.config.cluster.acquire_batch_size;
        loop {
            // 等待信号 (Wait)
            match pacemaker.wait_next().await {
                PacemakerEvent::Trigger => {}
                PacemakerEvent::Shutdown => break,
            }
            // 插件流控
            let mut allow_fetch = true;
            for plugin in self.inner.plugins.iter() {
                if !plugin.before_fetch(&self.inner.ctx).await {
                    allow_fetch = false;
                    break;
                }
            }
            if !allow_fetch {
                pacemaker.mark_idle();
                continue;
            }
            // =========================================================
            // 并发控制
            // =========================================================

            // 检查余量
            let available = self.inner.semaphore.available_permits();
            // 如果当前完全没空位，不要忙轮询，而是挂起等待至少 1 个空位释放。
            if available == 0 {
                match tokio::time::timeout(Duration::from_secs(1), self.inner.semaphore.acquire())
                    .await
                {
                    Ok(Ok(permit)) => {
                        // 等到了空位！立即释放
                        drop(permit);
                    }
                    _ => {
                        // 等了 1秒 还没空位，或者获取失败
                        // 标记为 idle，让 pacemaker 增加退避，减少检测频率
                        pacemaker.mark_idle();
                    }
                }
                continue;
            }

            // 计算本次最大能拉多少 (不超过 Batch，也不超过 Available)
            // 注意：再次获取 available，因为刚才可能变了
            let current_available = semaphore.available_permits();
            // 防止 acquire_many 报错（不能申请 0 个）
            if current_available == 0 {
                continue;
            }
            let ask_size = batch_size_cfg.min(current_available);

            // 批量占座 (Bulk Acquire)
            // 一次原子操作拿走所有需要的票。
            let bulk_permit = match semaphore.clone().acquire_many_owned(ask_size as u32).await {
                Ok(p) => p,
                Err(_) => break, // 信号量被关闭
            };
            // =========================================================
            // 执行拉取 (Fetch)
            // =========================================================

            // 调用持久化层 (Queue) 拉取任务
            match self
                .inner
                .ctx
                .queue
                .acquire(&self.inner.ctx.node_id, ask_size, 3)
                .await
            {
                Ok(items) => {
                    let fetched_count = items.len();

                    // 没有拉到任务，可能是暂时没有到期的任务了，或者竞争太激烈了。
                    if fetched_count == 0 {
                        pacemaker.mark_idle();
                        continue;
                    }
                    // 拉到了任务 -> 标记忙碌 (重置退避时间为 0)
                    pacemaker.mark_busy();
                    // 禁止自动释放许可，因为我们要把它们分发给具体的任务了。
                    bulk_permit.forget();

                    // 实际拿到的任务数量可能少于 ask_size，释放多余的许可
                    if fetched_count < ask_size {
                        semaphore.add_permits(ask_size - fetched_count);
                    }

                    // 任务分发 (Spawn)
                    for item in items {
                        let guard = PermitGuard {
                            semaphore: semaphore.clone(),
                            permits: 1,
                        };
                        self.spawn_task(item, guard).await;
                    }
                }
                Err(e) => {
                    error!("[Driver] Acquire failed: {:?}", e);
                    // 遇到错误必须退避，防止错误风暴
                    pacemaker.mark_idle();
                    // bulk_permit 自动 Drop，全额退票 -> 正确

                    // 保护性休眠
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// 任务分发
    async fn spawn_task(&self, item: AcquireItem, permit: PermitGuard) {
        let driver = self.clone();

        let token = CancellationToken::new();
        driver.inner.running_tasks.insert(
            item.id.clone(),
            RunningTaskHandle {
                epoch: item.score,
                token: token.clone(),
            },
        );
        tokio::spawn(async move {
            // 所有权转移：PermitGuard 移动到了这个 Future 内部。
            // 无论 execute_task 是成功、失败还是 Panic，
            // 只要这个 async 块结束，permit 就会被 Drop，从而归还信号量。
            let _guard = permit;
            driver.execute_task(item.id, item.score, token).await;
        });
    }

    // ==========================================
    // Core Logic: 执行逻辑
    // ==========================================

    /// 执行任务
    async fn execute_task(&self, task_id: String, epoch: u64, token: CancellationToken) {
        let store = &self.inner.ctx.store;
        let mut task_data = match store.load(&task_id).await {
            // 1. 正常路径
            Ok(LoadStatus::Found(c)) => c,
            // 2.  数据损坏
            Ok(LoadStatus::DataCorrupted {
                reason: _,
                raw_content: _,
            }) => {
                // 必须彻底删除该任务，否则 Rescuer 会无限复活它
                store.remove(&task_id).await.ok();
                self.inner.running_tasks.remove(&task_id);
                return;
            }
            // 3. 幽灵路径 (Orphan): 索引还在，数据没了
            Ok(LoadStatus::NotFound) => {
                // 彻底清理
                let _ = store.remove(&task_id).await;
                self.inner.running_tasks.remove(&task_id);
                return;
            }
            // 4. 系统故障: Redis 连不上等
            Err(e) => {
                error!("[Driver] Load task {} failed: {:?}", task_id, e);
                self.inner.running_tasks.remove(&task_id);
                return;
            }
        };
        let runtime_ctx = TaskContext::new(task_data.clone(), token.clone());

        task_data.mark_running(self.inner.ctx.node_id.clone(), epoch);
        // Hook: 执行前
        for p in self.inner.plugins.iter() {
            p.before_execute(&runtime_ctx).await;
        }
        let start_time = TimeUtils::now();
        let result = AssertUnwindSafe(self.inner.task_handler.execute(runtime_ctx.clone()))
            .catch_unwind()
            .await;
        let duration = TimeUtils::now() - start_time;
        // [Hook] 执行后 (通用)
        // 无论成功失败，先记录耗时
        for p in self.inner.plugins.iter() {
            p.after_execute(&runtime_ctx, duration).await;
        }
        // 处理执行结果
        match result {
            Ok(execution_result) => match execution_result {
                Ok(_) => {
                    // [Hook] 成功钩子
                    for p in self.inner.plugins.iter() {
                        p.on_success(&runtime_ctx).await;
                    }
                    self.handle_success(task_data).await
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    // [Hook] 失败钩子
                    for p in self.inner.plugins.iter() {
                        p.on_failure(&runtime_ctx, &err_msg).await;
                    }
                    self.handle_failure(task_data, err_msg).await
                }
            },
            Err(panic_err) => {
                let msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                    format!("Panic: {}", s)
                } else if let Some(s) = panic_err.downcast_ref::<String>() {
                    format!("Panic: {}", s)
                } else {
                    "Panic: Unknown error".to_string()
                };
                error!("[Driver] Task {} panicked: {}", task_id, msg);
                // [Hook] 失败钩子
                for p in self.inner.plugins.iter() {
                    p.on_failure(&runtime_ctx, &msg).await;
                }
                self.handle_failure(task_data, msg).await;
            }
        }
        self.inner.running_tasks.remove(&task_id);
    }

    /// 处理任务失败
    /// - 释放锁
    /// - 计算退避时间
    /// - 重入队或进入 DLQ
    async fn handle_failure(&self, mut task: TaskData<T>, err_msg: String) {
        let store = &self.inner.ctx.store;
        let queue = &self.inner.ctx.queue;

        // 释放队列锁
        let _ = queue
            .release(&[task.id.clone()], &self.inner.ctx.node_id)
            .await;
        if task.can_retry() {
            // === 可重试 ===
            task.last_error = Some(err_msg);

            // 使用带抖动的指数退避算法
            // 参数配置可以从 self.inner.ctx.config 中读取，这里先写死默认值：
            // - Base: 1秒
            // - Max: 1小时 (3600秒)
            let backoff_duration = calculate_backoff(
                task.attempt,
                1.0,    // Base Delay
                3600.0, // Max Delay
            );

            let now = TimeUtils::now();

            // 下次拉取时间 = 现在 + 退避时长
            task.next_poll_at = now + backoff_duration.as_secs_f64();

            // 重置状态以便下次被拉取
            task.state = TaskState::Pending;
            task.epoch = 0;
            task.worker_id = None;
            task.updated_at = now;
            // 重新入队
            if let Err(e) = store.requeue(task.clone()).await {
                error!("[Driver] Retry requeue failed for {}: {:?}", task.id, e);
            }
        } else {
            // === 重试耗尽 (Exhausted) ===
            if let Err(e) = store.move_to_dlq(&task, err_msg).await {
                error!("[Driver] Move to DLQ failed for {}: {:?}", task.id, e);
            }
        }
    }
    /// 运行成功
    async fn handle_success(&self, mut task: TaskData<T>) {
        let store = &self.inner.ctx.store;
        let queue = &self.inner.ctx.queue;
        // 释放运行锁
        let _ = queue
            .release(&[task.id.clone()], &self.inner.ctx.node_id)
            .await;
        let now = TimeUtils::now_f64();
        // 计算下次运行时间
        let next_time = TimeUtils::next_recurrence_time(
            &task.schedule_type,
            task.timezone.as_deref(), // 获取 Option<&str>
            now,
        );
        if let Some(ts) = next_time {
            task.next_poll_at = ts;
            task.state = TaskState::Pending;
            task.epoch = 0;
            task.worker_id = None;
            task.updated_at = now;
            // 重新入队
            if let Err(e) = store.requeue(task).await {
                error!("Requeue failed: {:?}", e);
            }
        } else {
            // === 是一次性任务，或 Cron 结束 ===
            if let Err(e) = store.remove(&task.id).await {
                error!("Remove failed: {:?}", e);
            }
        }
    }
    /// 心跳包循环
    async fn heartbeat_loop(&self) {
        // 配置系统间隔时间
        let interval = Duration::from_secs(self.inner.ctx.config.worker.heartbeat_interval_secs);

        loop {
            sleep(interval).await;
            if self.inner.ctx.is_shutdown() {
                break;
            }

            // 1. 获取快照
            let tasks: Vec<(String, u64)> = self
                .inner
                .running_tasks
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().epoch))
                .collect();

            if tasks.is_empty() {
                continue;
            }
            const BATCH_SIZE: usize = 100;
            for batch in tasks.chunks(BATCH_SIZE) {
                // 2. 发送心跳
                if let Err(e) = self
                    .inner
                    .ctx
                    .queue
                    .heartbeat(&batch, &self.inner.ctx.node_id)
                    .await
                {
                    trace!("[Driver] Heartbeat failed: {:?}", e);
                    // 可选：防止日志刷屏
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    /// 僵尸任务回收循环
    async fn rescue_loop(&self) {
        let config = &self.inner.ctx.config.policy;
        let interval = Duration::from_millis(config.zombie_check_interval_ms);

        if config.zombie_check_interval_ms == 0 {
            return;
        }

        loop {
            sleep(interval).await;
            if self.inner.ctx.is_shutdown() {
                break;
            }

            // 获取统一时间
            let now = TimeUtils::now();
            let timeout_ms = config.zombie_check_interval_ms; // 或者 visibility_timeout_ms
            let batch_size = config.rescue_batch_size;

            // 调用 rescue，传入 timeout 原始值，让底层决定怎么算
            match self
                .inner
                .ctx
                .queue
                .rescue(&self.inner.ctx.node_id, now, timeout_ms, batch_size)
                .await
            {
                Ok(ids) => {
                    if !ids.is_empty() {
                        // log...
                    }
                }
                Err(e) => error!("Rescue failed: {:?}", e),
            }
        }
    }
}

impl<Task, T> TaskDriver<Task, T>
where
    Task: crate::common::traits::SchedulableTask<T> + Send + Sync + 'static,
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    pub fn cancel_task(&self, task_id: &str) -> bool {
        if let Some(handle) = self.inner.running_tasks.get(task_id) {
            handle.token.cancel(); // 触发级联取消
            trace!("[Driver] Cancel signal sent to task {}", task_id);
            return true;
        }
        false
    }

    pub fn cancel_all_running(&self) -> usize {
        let mut count = 0;
        // 同样，这里也不要 remove，只是触发 cancel
        for entry in self.inner.running_tasks.iter() {
            entry.value().token.cancel();
            count += 1;
        }
        trace!("[Driver] Broadcast cancel to {} tasks.", count);
        count
    }
    /// 触发优雅停机
    /// - 所有组件通过 CancellationToken 收到通知
    pub fn shutdown(&self) {
        trace!("[Driver] Shutdown triggered.");
        self.inner.ctx.shutdown.cancel();
    }
}
