package com.hazelcast.auditlog.impl;

import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.AuditlogServiceFactory;
import com.hazelcast.security.LoggingServiceCallback;

public class DefaultAuditlogFactory implements AuditlogServiceFactory {

    private volatile CallbackHandler callbackHandler;

    @Override
    public void init(CallbackHandler callbackHandler, Properties properties) throws Exception {
        this.callbackHandler = callbackHandler;
    }

    @Override
    public AuditlogService createAuditlog() throws Exception {
        LoggingServiceCallback lscb = new LoggingServiceCallback();
        callbackHandler.handle(new Callback[]{lscb});
        return new ILoggerAuditlogService(lscb.getLoggingService());
    }

}
