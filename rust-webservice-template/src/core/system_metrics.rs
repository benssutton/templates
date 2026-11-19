//! Process and host resource snapshot (sysinfo).
//!
//! CPU percentages are deltas since the previous refresh, so `new()` primes
//! once (the first reading would otherwise be 0). `num_threads` uses
//! `Process::tasks()`, which sysinfo provides on Linux only; elsewhere it is
//! 0, as is `open_files` where the OS does not expose it.

use crate::schemas::health::{HostStats, ProcessStats, SystemSnapshot};
use std::sync::{Mutex, PoisonError};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

pub struct SystemMetrics {
    system: Mutex<System>,
    pid: Option<Pid>,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemMetrics {
    pub fn new() -> Self {
        let pid = sysinfo::get_current_pid().ok();
        let mut system = System::new();
        refresh(&mut system, pid); // prime the CPU deltas
        Self {
            system: Mutex::new(system),
            pid,
        }
    }

    pub fn snapshot(&self) -> SystemSnapshot {
        let mut system = self.system.lock().unwrap_or_else(PoisonError::into_inner);
        refresh(&mut system, self.pid);

        let process = self.pid.and_then(|pid| system.process(pid));
        let process_stats = ProcessStats {
            cpu_percent: process.map_or(0.0, |p| f64::from(p.cpu_usage())),
            memory_rss_bytes: process.map_or(0, |p| p.memory()),
            num_threads: process
                .and_then(|p| p.tasks())
                .map_or(0, |tasks| tasks.len() as u64),
            open_files: process.and_then(|p| p.open_files()).map_or(0, |n| n as u64),
        };

        let total = system.total_memory();
        let available = system.available_memory();
        let memory_percent = if total == 0 {
            0.0
        } else {
            (total.saturating_sub(available) as f64 / total as f64) * 100.0
        };
        SystemSnapshot {
            process: process_stats,
            host: HostStats {
                cpu_percent: f64::from(system.global_cpu_usage()),
                memory_total_bytes: total,
                memory_available_bytes: available,
                memory_percent,
            },
        }
    }

    /// Host boot time in unix seconds.
    pub fn boot_time_seconds() -> f64 {
        System::boot_time() as f64
    }
}

fn refresh(system: &mut System, pid: Option<Pid>) {
    system.refresh_memory();
    system.refresh_cpu_usage();
    if let Some(pid) = pid {
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory()
                .with_tasks(),
        );
    }
}
