package com.hazelcast.security.auditlog;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.security.auth.callback.CallbackHandler;

import com.hazelcast.auditlog.AuditableEvent;
import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.EventBuilder;
import com.hazelcast.auditlog.Level;
import com.hazelcast.auditlog.impl.SimpleEvent;

public class TestAuditlogService implements AuditlogService {

    private final CallbackHandler callbackHandler;
    private final Properties properties;
    private final ConcurrentLinkedQueue<AuditableEvent> eventQueue = new ConcurrentLinkedQueue<AuditableEvent>();

    public TestAuditlogService(CallbackHandler callbackHandler, Properties properties) {
        this.callbackHandler = callbackHandler;
        this.properties = properties;
    }

    @Override
    public void log(AuditableEvent auditableEvent) {
        if (auditableEvent instanceof DumpTestAuditlogEvent) {
            ((DumpTestAuditlogEvent) auditableEvent).setTestAuditlogService(this);
            return;
        }
        eventQueue.add(auditableEvent);
    }

    @Override
    public void log(String eventTypeId, Level level, String message) {
        log(eventBuilder(eventTypeId).level(level).message(message).build());
    }

    @Override
    public void log(String eventTypeId, Level level, String message, Throwable thrown) {
        log(eventBuilder(eventTypeId).level(level).message(message).cause(thrown).build());
    }

    @Override
    public EventBuilder<?> eventBuilder(String typeId) {
        return SimpleEvent.builder(typeId, this);
    }

    public CallbackHandler getCallbackHandler() {
        return callbackHandler;
    }

    public Properties getProperties() {
        return properties;
    }

    public ConcurrentLinkedQueue<AuditableEvent> getEventQueue() {
        return eventQueue;
    }

    public void assertEventPresent(String... expectedTypeIds) {
        for (String expectedTypeId : expectedTypeIds) {
            assertTrue(eventQueue.stream().map(e -> e.typeId()).anyMatch(s -> expectedTypeId.equals(s)));
        }
    }

    public void assertEventsNotPresent(String... typeIds) {
        for (String typeId : typeIds) {
            assertTrue("Unexpected event " + typeId, eventQueue.stream().map(e -> e.typeId()).noneMatch(s -> typeId.equals(s)));
        }
    }

    public boolean hasEvent(String typeId) {
        return eventQueue.stream().map(e -> e.typeId()).anyMatch(s -> typeId.equals(s));
    }

    public void assertEventPresentEventually(String... expectedTypeIds) {
        assertTrueEventually(() -> assertEventPresent(expectedTypeIds));
    }
}
