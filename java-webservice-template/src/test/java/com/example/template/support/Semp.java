package com.example.template.support;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Thin SEMP v2 client (mirrors python tests/support/semp.py): broker-side facts for assertions. */
public final class Semp {
    private static final Pattern MSG_ID = Pattern.compile("\"msgId\"\\s*:");
    private static final Pattern SUB_TOPIC = Pattern.compile("\"subscriptionTopic\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern CLIENT_NAME = Pattern.compile("\"clientName\"\\s*:");
    private static final Pattern FLOW_ID = Pattern.compile("\"flowId\"\\s*:");
    private static final Pattern UNACKED = Pattern.compile("\"unackedMsgCount\"\\s*:\\s*(\\d+)");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    private final String base;
    private final String auth;

    public Semp(String baseUrl) {
        this.base = baseUrl + "/SEMP/v2";
        this.auth = "Basic " + Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8));
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    private HttpResponse<String> send(String method, String path, String json) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
            .timeout(TIMEOUT)
            .header("Authorization", auth);
        HttpRequest.BodyPublisher body = json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json);
        if (json != null) b.header("Content-Type", "application/json");
        try {
            return http.send(b.method(method, body).build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IllegalStateException("SEMP " + method + " " + path, e);
        }
    }

    private HttpResponse<String> ok(HttpResponse<String> r) {
        if (r.statusCode() / 100 != 2) throw new IllegalStateException("SEMP " + r.statusCode() + ": " + r.body());
        return r;
    }

    public boolean queueExists(String queue) {
        return send("GET", "/config/msgVpns/default/queues/" + enc(queue), null).statusCode() == 200;
    }

    public List<String> queueSubscriptions(String queue) {
        Matcher m = SUB_TOPIC.matcher(ok(send("GET", "/config/msgVpns/default/queues/" + enc(queue) + "/subscriptions", null)).body());
        List<String> out = new ArrayList<>();
        while (m.find()) out.add(m.group(1));
        return out;
    }

    /** Messages currently spooled on the queue, including delivered-but-unacked. */
    public int queueMessageCount(String queue) {
        return count(MSG_ID, "/monitor/msgVpns/default/queues/" + enc(queue) + "/msgs?count=100");
    }

    private int count(Pattern key, String path) {
        Matcher m = key.matcher(ok(send("GET", path, null)).body());
        int n = 0;
        while (m.find()) n++;
        return n;
    }

    /** Consumer flows currently bound to the queue. */
    public int queueBindCount(String queue) {
        return count(FLOW_ID, "/monitor/msgVpns/default/queues/" + enc(queue) + "/txFlows");
    }

    /** Messages delivered to the queue's consumer flows but not yet settled. */
    public int queueUnackedCount(String queue) {
        Matcher m = UNACKED.matcher(ok(send("GET", "/monitor/msgVpns/default/queues/" + enc(queue) + "/txFlows", null)).body());
        int n = 0;
        while (m.find()) n += Integer.parseInt(m.group(1));
        return n;
    }

    /** Connected clients with exactly this client name. */
    public int clientCount(String clientName) {
        return count(CLIENT_NAME, "/monitor/msgVpns/default/clients?where=" + enc("clientName==" + clientName));
    }

    public void createEndpointDeniedUser(String profile, String user, String password) {
        ok(send("POST", "/config/msgVpns/default/clientProfiles",
            "{\"clientProfileName\":\"" + profile + "\",\"allowGuaranteedMsgReceiveEnabled\":true,"
                + "\"allowGuaranteedMsgSendEnabled\":true,\"allowGuaranteedEndpointCreateEnabled\":false}"));
        ok(send("POST", "/config/msgVpns/default/clientUsernames",
            "{\"clientUsername\":\"" + user + "\",\"password\":\"" + password + "\",\"clientProfileName\":\""
                + profile + "\",\"aclProfileName\":\"default\",\"enabled\":true}"));
    }
}
