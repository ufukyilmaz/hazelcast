package com.hazelcast.session;

import org.apache.catalina.Session;

public interface SessionManager {

    public void remove(Session session);

    public void commit(Session session);

}
