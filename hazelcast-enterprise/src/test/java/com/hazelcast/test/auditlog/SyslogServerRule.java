package com.hazelcast.test.auditlog;

import static org.junit.Assert.fail;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

import java.net.Socket;
import java.util.Queue;

import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.impl.net.tcp.TCPNetSyslogServer;
import org.graylog2.syslog4j.server.impl.net.tcp.TCPNetSyslogServerConfig;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.hazelcast.test.HazelcastTestSupport;

/**
 * JUnit {@link org.junit.Rule} which starts a Syslog server (Syslog4j) on the given port.
 */
public final class SyslogServerRule implements TestRule {

    private final int port;
    private final SyslogQueueMessageHandler eventHandler = new SyslogQueueMessageHandler();

    private SyslogServerRule(int port) {
        this.port = port;
    }

    public static SyslogServerRule create(int port) {
        return new SyslogServerRule(port);
    }

    public int getPort() {
        return port;
    }

    public Queue<SyslogServerEventIF> getEventQueue() {
        return eventHandler.getQueue();
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                TCPNetSyslogServerConfig cfg = new TCPNetSyslogServerConfig("127.0.0.1", port);
                cfg.addEventHandler(eventHandler);
                TCPNetSyslogServer server = new TCPNetSyslogServer();
                server.initialize("tcp", cfg);
                Thread thread = new Thread(server);
                thread.setName("TCPNetSyslogServer");
                thread.setDaemon(true);
                thread.start();
                assertTrueEventually(() -> assertSyslogListening());
                try {
                    base.evaluate();
                } finally {
                    server.shutdown();
                }
            }
        };
    }

    private void assertSyslogListening() {
        try (Socket s = new Socket("127.0.0.1", port)) {
            return;
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void assertSyslogContainsEventually(String... typeIds) {
        HazelcastTestSupport.assertTrueEventually(() -> assertSyslogContains(typeIds));
    }

    public void assertSyslogContains(String... typeIds) {
        for (String type : typeIds) {
            Queue<SyslogServerEventIF> queue = getEventQueue();
            assertTrue("Type should be present in the Syslog: " + type,
                    queue.stream().anyMatch(se -> se.getMessage().contains(type)));
        }
    }

}
