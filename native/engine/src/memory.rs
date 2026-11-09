use crate::*;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// DataFusion memory pool backed by the TaskManager-wide JVM reservation authority.
pub(crate) struct FlinkTaskOffHeapPool {
    owner_id: i64,
    used: AtomicUsize,
    authority: jni::objects::GlobalRef,
    owns_registration: bool,
    spill_headroom: AtomicUsize,
}

impl FlinkTaskOffHeapPool {
    fn new(owner_id: i64) -> Result<Self, DataFusionError> {
        let vm = crate::bridge::JVM.get().ok_or_else(|| {
            DataFusionError::Execution("JVM memory authority is unavailable".into())
        })?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let class = env
            .find_class("tech/streamfusion/operator/TaskOffHeapMemory")
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let authority = env
            .new_global_ref(class)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self {
            owner_id,
            used: AtomicUsize::new(0),
            authority,
            owns_registration: false,
            spill_headroom: AtomicUsize::new(0),
        })
    }

    pub(crate) fn preserve_spill_headroom(&self, bytes: usize) {
        self.spill_headroom.fetch_max(bytes, Relaxed);
    }

    fn reserve(&self, bytes: usize, can_spill: bool) -> Result<bool, DataFusionError> {
        let vm = crate::bridge::JVM.get().ok_or_else(|| {
            DataFusionError::Execution("JVM memory authority is unavailable".into())
        })?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let headroom = if can_spill {
            self.spill_headroom.load(Relaxed)
        } else {
            0
        };
        if headroom != 0 {
            return env
                .call_static_method(
                    <&jni::objects::JClass>::from(self.authority.as_obj()),
                    "tryReserveWithHeadroom",
                    "(JJJ)Z",
                    &[
                        jni::objects::JValue::Long(self.owner_id),
                        jni::objects::JValue::Long(bytes as i64),
                        jni::objects::JValue::Long(headroom as i64),
                    ],
                )
                .and_then(|value| value.z())
                .map_err(|e| DataFusionError::Execution(e.to_string()));
        }
        env.call_static_method(
            <&jni::objects::JClass>::from(self.authority.as_obj()),
            "tryReserve",
            "(JJ)Z",
            &[
                jni::objects::JValue::Long(self.owner_id),
                jni::objects::JValue::Long(bytes as i64),
            ],
        )
        .and_then(|value| value.z())
        .map_err(|e| DataFusionError::Execution(format!("reserve task off-heap memory: {e}")))
    }

    fn release(&self, bytes: usize) -> Result<(), DataFusionError> {
        let vm = crate::bridge::JVM.get().ok_or_else(|| {
            DataFusionError::Execution("JVM memory authority is unavailable".into())
        })?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        env.call_static_method(
            <&jni::objects::JClass>::from(self.authority.as_obj()),
            "release",
            "(JJ)V",
            &[
                jni::objects::JValue::Long(self.owner_id),
                jni::objects::JValue::Long(bytes as i64),
            ],
        )
        .map(|_| ())
        .map_err(|e| DataFusionError::Execution(format!("release task off-heap memory: {e}")))
    }
}

impl Drop for FlinkTaskOffHeapPool {
    fn drop(&mut self) {
        if !self.owns_registration {
            return;
        }
        // Reservations and DataFusion worker contexts retain the pool. Closing the owner here
        // keeps async cancellation charged until its final worker releases memory.
        let result = (|| {
            let vm = crate::bridge::JVM.get().ok_or_else(|| {
                DataFusionError::Execution("JVM memory authority is unavailable".into())
            })?;
            let mut env = vm
                .attach_current_thread()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            env.call_static_method(
                <&jni::objects::JClass>::from(self.authority.as_obj()),
                "closeOwner",
                "(J)V",
                &[jni::objects::JValue::Long(self.owner_id)],
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            Ok::<(), DataFusionError>(())
        })();
        if let Err(error) = result {
            log::error!("close native task memory owner {}: {error}", self.owner_id);
        }
    }
}

/// The native bounded task owns this registration through the pool's last reference, including
/// worker tasks cancelled asynchronously by DataFusion.
pub(crate) fn owned_task_memory_pool(name: &str) -> Result<Arc<dyn MemoryPool>, DataFusionError> {
    let mut pool = FlinkTaskOffHeapPool::new(0)?;
    let vm = crate::bridge::JVM.get().unwrap();
    let mut env = vm
        .attach_current_thread()
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let category = env
        .new_string("batch")
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let name = env
        .new_string(name)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    pool.owner_id = env
        .call_static_method(
            <&jni::objects::JClass>::from(pool.authority.as_obj()),
            "registerOwner",
            "(Ljava/lang/String;Ljava/lang/String;)J",
            &[
                jni::objects::JValue::Object(&category),
                jni::objects::JValue::Object(&name),
            ],
        )
        .and_then(|value| value.j())
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    pool.owns_registration = true;
    Ok(Arc::new(pool))
}

impl Debug for FlinkTaskOffHeapPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FlinkTaskOffHeapPool")
            .field("owner_id", &self.owner_id)
            .field("used", &self.used.load(Relaxed))
            .finish()
    }
}

impl Display for FlinkTaskOffHeapPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "FlinkTaskOffHeapPool(owner={}, used={})",
            self.owner_id,
            self.used.load(Relaxed)
        )
    }
}

impl MemoryPool for FlinkTaskOffHeapPool {
    fn name(&self) -> &str {
        "FlinkTaskOffHeapPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap();
    }

    fn shrink(&self, _reservation: &MemoryReservation, size: usize) {
        self.release(size).expect("return task off-heap memory");
        self.used.fetch_sub(size, Relaxed);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional == 0 {
            return Ok(());
        }
        if !self.reserve(additional, reservation.consumer().can_spill())? {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "TaskManager task off-heap pool denied {additional} bytes for owner {}",
                self.owner_id
            )));
        }
        self.used.fetch_add(additional, Relaxed);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Relaxed)
    }
}

/// Managed-memory accounting for one native operator handle: a reservation against a bounded pool
/// sized by the budget the host reserved for the operator, plus the incrementally tracked estimate
/// of the state's heap footprint. Unaccounted (no budget) it is inert — the tracking branches cost
/// one predicted-false test per touch. `account()` resizes the reservation to the tracked bytes; a
/// denial is the budget-exceeded signal, surfaced to the host as a clear failure instead of the
/// container OOM-killing the process (these operators have no runtime spill to fall back to).
pub(crate) struct OperatorMemory {
    reservation: Option<MemoryReservation>,
    pub(crate) state_bytes: usize,
    // A TaskContext sharing the bounded pool, for fragments the operator delegates to DataFusion's
    // execution (the joins' HashJoinExec) — their transient working memory then draws on the same
    // budget as the operator's state.
    task_ctx: Option<Arc<TaskContext>>,
}

impl OperatorMemory {
    pub(crate) fn unaccounted() -> Self {
        OperatorMemory {
            reservation: None,
            state_bytes: 0,
            task_ctx: None,
        }
    }

    /// Attaches either a standalone byte cap (non-negative, used by native tests) or an encoded JVM
    /// owner (below -1, production), accounting restored state immediately. -1 is unaccounted.
    pub(crate) fn attach(
        &mut self,
        consumer: &str,
        budget_bytes: i64,
        current_state_bytes: usize,
    ) -> Result<(), DataFusionError> {
        if budget_bytes == -1 {
            return Ok(());
        }
        let pool: Arc<dyn MemoryPool> = if budget_bytes < -1 {
            Arc::new(FlinkTaskOffHeapPool::new(-budget_bytes - 1)?)
        } else {
            Arc::new(GreedyMemoryPool::new(budget_bytes as usize))
        };
        self.attach_pool(consumer, &pool, current_state_bytes)
    }

    /// [`attach`](Self::attach) against a caller-owned pool (shared in tests to observe the pool's
    /// balance from outside).
    pub(crate) fn attach_pool(
        &mut self,
        consumer: &str,
        pool: &Arc<dyn MemoryPool>,
        current_state_bytes: usize,
    ) -> Result<(), DataFusionError> {
        self.reservation = Some(MemoryConsumer::new(consumer.to_string()).register(pool));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(pool))
            .build_arc()?;
        self.task_ctx = Some(Arc::new(TaskContext::default().with_runtime(runtime)));
        self.state_bytes = current_state_bytes;
        self.account()
    }

    /// Whether a budget is attached — gate for any per-touch measurement work.
    pub(crate) fn tracking(&self) -> bool {
        self.reservation.is_some()
    }

    pub(crate) fn temporary_reservation(&self) -> Option<MemoryReservation> {
        self.reservation.as_ref().map(MemoryReservation::new_empty)
    }

    /// The TaskContext DataFusion-executed fragments must run under: pool-bounded when a budget is
    /// attached, a plain default (unbounded, as before accounting) otherwise.
    pub(crate) fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone().unwrap_or_default()
    }

    /// Folds a touched entry's footprint change into the tracked total.
    pub(crate) fn record(&mut self, delta: isize) {
        self.state_bytes = self.state_bytes.saturating_add_signed(delta);
    }

    /// Removes a dropped entry's footprint (an eviction or flush).
    pub(crate) fn forget(&mut self, bytes: usize) {
        self.state_bytes = self.state_bytes.saturating_sub(bytes);
    }

    /// Replaces the tracked total — for mutation paths that rebuild whole containers (an eviction
    /// that reslices buffered batches) where recomputing is cheaper than delta bookkeeping.
    pub(crate) fn set(&mut self, bytes: usize) {
        self.state_bytes = bytes;
    }

    pub(crate) fn account(&mut self) -> Result<(), DataFusionError> {
        let Some(reservation) = &mut self.reservation else {
            return Ok(());
        };
        reservation.try_resize(self.state_bytes).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "native operator state exceeded TaskManager task off-heap memory; raise \
                 taskmanager.memory.task.off-heap.size ({e})"
            ))
        })
    }

    /// `account()` on a path where the tracked size can only have shrunk.
    pub(crate) fn account_shrink(&mut self) {
        self.account().expect("shrinking a reservation cannot fail");
    }
}

/// Total Arrow buffer footprint of a set of buffered batches.
pub(crate) fn buffered_batches_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}
