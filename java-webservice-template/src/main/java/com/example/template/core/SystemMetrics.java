package com.example.template.core;

import com.example.template.dto.health.HostStats;
import com.example.template.dto.health.ProcessStats;
import com.example.template.dto.health.SystemSnapshot;
import jakarta.inject.Singleton;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Process/host resource snapshot — the {@code system_metrics.py}/psutil analogue via JMX.
 * {@code open_files} has no portable JMX equivalent and is reported as 0 (documented divergence).
 */
@Singleton
public class SystemMetrics {

    private final com.sun.management.OperatingSystemMXBean os =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final Runtime runtime = Runtime.getRuntime();

    public SystemSnapshot snapshot() {
        long totalMem = os.getTotalMemorySize();
        long freeMem = os.getFreeMemorySize();
        double hostCpu = os.getCpuLoad() < 0 ? 0.0 : os.getCpuLoad() * 100.0;
        double procCpu = os.getProcessCpuLoad() < 0 ? 0.0 : os.getProcessCpuLoad() * 100.0;
        long rss = runtime.totalMemory() - runtime.freeMemory();
        double usedPct = totalMem > 0 ? (totalMem - freeMem) * 100.0 / totalMem : 0.0;

        ProcessStats process = new ProcessStats(procCpu, rss, threads.getThreadCount(), 0);
        HostStats host = new HostStats(hostCpu, totalMem, freeMem, usedPct);
        return new SystemSnapshot(process, host);
    }
}
