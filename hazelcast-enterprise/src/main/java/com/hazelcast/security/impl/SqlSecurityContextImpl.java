package com.hazelcast.security.impl;

import com.hazelcast.security.SecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.security.auth.Subject;
import java.security.Permission;

public class SqlSecurityContextImpl implements SqlSecurityContext {

    private final SecurityContext context;
    private final Subject subject;

    public SqlSecurityContextImpl(SecurityContext context, Subject subject) {
        this.context = context;
        this.subject = subject;
    }

    @Override
    public boolean isSecurityEnabled() {
        return true;
    }

    @Override
    public void checkPermission(Permission permission) {
        context.checkPermission(subject, permission);
    }
}
