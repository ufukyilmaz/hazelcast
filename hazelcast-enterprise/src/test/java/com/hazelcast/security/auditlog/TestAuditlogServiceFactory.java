package com.hazelcast.security.auditlog;

import java.util.Properties;

import javax.security.auth.callback.CallbackHandler;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.AuditlogServiceFactory;

public class TestAuditlogServiceFactory implements AuditlogServiceFactory {

    private volatile CallbackHandler callbackHandler;
    private volatile Properties properties;

    @Override
    public void init(CallbackHandler callbackHandler, Properties properties) throws Exception {
        this.callbackHandler = callbackHandler;
        this.properties = properties;
    }

    @Override
    public AuditlogService createAuditlog() throws Exception {
        return new TestAuditlogService(callbackHandler, properties);
    }

}
