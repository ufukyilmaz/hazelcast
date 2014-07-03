package com.hazelcast.session;

import org.apache.catalina.Session;

import java.io.IOException;

public interface SessionManager {

    String DEFAULT_INSTANCE_NAME = "SESSION-REPLICATION-INSTANCE";

    void remove(Session session);

    void commit(Session session);

    String updateJvmRouteForSession(String sessionId, String newJvmRoute) throws IOException;

    String getJvmRoute();

}
