package com.hazelcast.session;

import org.apache.catalina.Session;

public interface SessionManager {

    void remove(Session session);

    void commit(Session session);

}
