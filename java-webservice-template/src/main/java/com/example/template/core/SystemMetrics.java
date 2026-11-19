package com.example.template.core;

import com.example.template.dto.health.HostStats;
import com.example.template.dto.health.ProcessStats;
import com.example.template.dto.health.SystemSnapshot;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Process/host resource snapshot via JMX, plus two Linux-only facts JMX lacks, read from {@code
 * /proc}: resident set size and host boot time. Where {@code /proc} is absent (macOS/Windows dev
 * machines) those fields are null rather than a different quantity under the same name. {@code
 * open_files} has no portable source and is reported as 0.
 */
@Singleton
public class SystemMetrics {

    private static final Path PROC_STATUS = Path.of("/proc/self/status");
    private static final Path PROC_STAT = Path.of("/proc/stat");

    private final com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    public SystemSnapshot snapshot() {
        long totalMem = os.getTotalMemorySize();
        long freeMem = os.getFreeMemorySize();
        double hostCpu = os.getCpuLoad() < 0 ? 0.0 : os.getCpuLoad() * 100.0;
        double procCpu = os.getProcessCpuLoad() < 0 ? 0.0 : os.getProcessCpuLoad() * 100.0;
        double usedPct = totalMem > 0 ? (totalMem - freeMem) * 100.0 / totalMem : 0.0;

        ProcessStats process =
                new ProcessStats(procCpu, residentSetBytes(), threads.getThreadCount(), 0);
        HostStats host = new HostStats(hostCpu, totalMem, freeMem, usedPct);
        return new SystemSnapshot(process, host);
    }

    /** Seconds since the JVM started (the process, not this bean). */
    public double processUptimeSeconds() {
        return Math.round(ManagementFactory.getRuntimeMXBean().getUptime() / 10.0) / 100.0;
    }

    /** Host boot time as epoch seconds ({@code btime} in /proc/stat), or null off Linux. */
    @Nullable
    public Double systemBootEpochSeconds() {
        Long btime = procField(PROC_STAT, "btime", 1);
        return btime == null ? null : btime.doubleValue();
    }

    /** {@code VmRSS} from /proc/self/status (reported in kB), or null off Linux. */
    @Nullable
    static Long residentSetBytes() {
        return procField(PROC_STATUS, "VmRSS:", 1024);
    }

    @Nullable
    private static Long procField(Path file, String key, long multiplier) {
        if (!Files.isReadable(file)) {
            return null;
        }
        try {
            List<String> lines = Files.readAllLines(file);
            for (String line : lines) {
                if (line.startsWith(key)) {
                    String[] parts = line.substring(key.length()).trim().split("\s+");
                    return Long.parseLong(parts[0]) * multiplier;
                }
            }
        } catch (IOException | RuntimeException e) {
            return null;
        }
        return null;
    }
}
