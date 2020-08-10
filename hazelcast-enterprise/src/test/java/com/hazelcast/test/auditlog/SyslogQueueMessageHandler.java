package com.hazelcast.test.auditlog;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.SyslogServerSessionlessEventHandlerIF;

public class SyslogQueueMessageHandler implements SyslogServerSessionlessEventHandlerIF {

    private static final long serialVersionUID = 1L;

    private final Queue<SyslogServerEventIF> queue = new LinkedBlockingQueue<>();

    public Queue<SyslogServerEventIF> getQueue() {
        return queue;
    }

    @Override
    public void initialize(SyslogServerIF syslogServer) {
        System.out.println(">>> SYSLOG Handler init");
    }

    @Override
    public void destroy(SyslogServerIF syslogServer) {
        System.out.println(">>> SYSLOG Handler destroy");
        queue.clear();
    }

    @Override
    public void event(SyslogServerIF syslogServer, SocketAddress socketAddress, SyslogServerEventIF event) {
        System.out.println(">>> SYSLOG Event: " + event.getMessage());
        queue.add(event);
    }

    @Override
    public void exception(SyslogServerIF syslogServer, SocketAddress socketAddress, Exception exception) {
        System.out.println(">>> SYSLOG Handler exception");
        exception.printStackTrace(System.out);
    }

}
