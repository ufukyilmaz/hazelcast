package com.hazelcast.security.auditlog;

import java.util.Map;

import com.hazelcast.auditlog.AuditableEvent;
import com.hazelcast.auditlog.Level;

public class DumpTestAuditlogEvent implements AuditableEvent {

    private TestAuditlogService testAuditlogService;

    @Override
    public String typeId() {
        return "dump";
    }

    @Override
    public String message() {
        return null;
    }

    @Override
    public Map<String, Object> parameters() {
        return null;
    }

    @Override
    public Level level() {
        return Level.INFO;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }

    public TestAuditlogService getTestAuditlogService() {
        return testAuditlogService;
    }

    public void setTestAuditlogService(TestAuditlogService testAuditlogService) {
        this.testAuditlogService = testAuditlogService;
    }
}
