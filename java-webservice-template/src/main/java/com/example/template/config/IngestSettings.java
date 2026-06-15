package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;

/** Ingest/transport configuration (mirrors the Python ingest settings). */
@ConfigurationProperties("template.ingest")
public class IngestSettings {
    private String transport = "flight";
    private long maxBatchBytes = 16L * 1024 * 1024;
    private long maxDisconnectSeconds = 60;
    private double stalenessThresholdSeconds = 0;
    private Flight flight = new Flight();
    private Solace solace = new Solace();

    public String getTransport() { return transport; }
    public void setTransport(String v) { this.transport = v; }
    public long getMaxBatchBytes() { return maxBatchBytes; }
    public void setMaxBatchBytes(long v) { this.maxBatchBytes = v; }
    public long getMaxDisconnectSeconds() { return maxDisconnectSeconds; }
    public void setMaxDisconnectSeconds(long v) { this.maxDisconnectSeconds = v; }
    public double getStalenessThresholdSeconds() { return stalenessThresholdSeconds; }
    public void setStalenessThresholdSeconds(double v) { this.stalenessThresholdSeconds = v; }
    public Flight getFlight() { return flight; }
    public void setFlight(Flight v) { this.flight = v; }
    public Solace getSolace() { return solace; }
    public void setSolace(Solace v) { this.solace = v; }

    @ConfigurationProperties("flight")
    public static class Flight {
        private String host = "localhost";
        private int port = 8815;
        private String ticket = "items";
        public String getHost() { return host; }
        public void setHost(String v) { this.host = v; }
        public int getPort() { return port; }
        public void setPort(int v) { this.port = v; }
        public String getTicket() { return ticket; }
        public void setTicket(String v) { this.ticket = v; }
    }

    @ConfigurationProperties("solace")
    public static class Solace {
        private String host = "localhost";
        private int port = 55555;
        private String vpn = "default";
        private String username = "admin";
        private String password = "admin";
        private String topic = "ingest/batches";
        public String getHost() { return host; }
        public void setHost(String v) { this.host = v; }
        public int getPort() { return port; }
        public void setPort(int v) { this.port = v; }
        public String getVpn() { return vpn; }
        public void setVpn(String v) { this.vpn = v; }
        public String getUsername() { return username; }
        public void setUsername(String v) { this.username = v; }
        public String getPassword() { return password; }
        public void setPassword(String v) { this.password = v; }
        public String getTopic() { return topic; }
        public void setTopic(String v) { this.topic = v; }
    }
}
