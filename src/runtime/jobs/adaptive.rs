use crate::config::AdaptiveScanSettings;
use crate::storage::VideoRegistry;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;
use tracing::{info, warn};

pub(crate) struct AdaptiveScanLimiter {
    semaphore: Arc<Semaphore>,
    held: Mutex<Vec<OwnedSemaphorePermit>>,
    max: usize,
    target: AtomicUsize,
}

impl AdaptiveScanLimiter {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            held: Mutex::new(Vec::new()),
            max,
            target: AtomicUsize::new(max),
        }
    }

    pub(crate) async fn acquire(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }

    pub(crate) async fn set_target(&self, target: usize) {
        let target = target.clamp(1, self.max);
        self.target.store(target, Ordering::Relaxed);
        let desired_held = self.max.saturating_sub(target);
        let mut held = self.held.lock().await;
        while held.len() > desired_held {
            held.pop();
        }
        while held.len() < desired_held {
            match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => held.push(permit),
                Err(_) => break,
            }
        }
    }

    pub(crate) fn current_target(&self) -> usize {
        self.target.load(Ordering::Relaxed)
    }
}

pub(crate) fn spawn_adaptive_controller(
    registry: VideoRegistry,
    settings: AdaptiveScanSettings,
    limiter: Arc<AdaptiveScanLimiter>,
) {
    tokio::spawn(async move {
        let min = std::cmp::max(1, settings.min_concurrent) as usize;
        let max = std::cmp::max(min, settings.max_concurrent as usize);
        let mut target = min;
        limiter.set_target(target).await;
        let interval = Duration::from_millis(std::cmp::max(200, settings.check_interval_ms));
        loop {
            sleep(interval).await;
            let queued_result = tokio::task::spawn_blocking({
                let registry = registry.clone();
                move || registry.count_jobs_by_type_and_status("scan-directory", "queued")
            })
            .await;
            let running_result = tokio::task::spawn_blocking({
                let registry = registry.clone();
                move || registry.count_jobs_by_type_and_status("scan-directory", "running")
            })
            .await;

            let queued = match queued_result {
                Ok(Ok(count)) => count,
                Ok(Err(e)) => {
                    warn!("[Jobs] Adaptive scan queued count failed: {}", e);
                    continue;
                }
                Err(e) => {
                    warn!("[Jobs] Adaptive scan queued count join error: {}", e);
                    continue;
                }
            };
            let running = match running_result {
                Ok(Ok(count)) => count,
                Ok(Err(e)) => {
                    warn!("[Jobs] Adaptive scan running count failed: {}", e);
                    continue;
                }
                Err(e) => {
                    warn!("[Jobs] Adaptive scan running count join error: {}", e);
                    continue;
                }
            };

            let mut desired = target;
            if queued >= target && target < max {
                desired = std::cmp::min(max, target + settings.step_up as usize);
            } else if queued == 0 && running < target {
                desired = target.saturating_sub(settings.step_down as usize).max(min);
            }

            if desired != target {
                target = desired;
                limiter.set_target(target).await;
                info!(
                    "[Jobs] Adaptive scan concurrency set to {} (queued {}, running {})",
                    target, queued, running
                );
            } else if limiter.current_target() != target {
                limiter.set_target(target).await;
            }
        }
    });
}
