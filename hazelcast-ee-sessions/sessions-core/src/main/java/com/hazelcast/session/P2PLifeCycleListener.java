package com.hazelcast.session;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleListener;


public class P2PLifeCycleListener implements LifecycleListener {

    public static String DEFAULT_INSTANCE_NAME = "SESSION-REPLICATION-INSTANCE";

    @Override
    public void lifecycleEvent(LifecycleEvent event) {

        String shutdown = System.getProperty("hazelcast.tomcat.shutdown_hazelcast_instance");

        if ("start".equals(event.getType())){
            Config config = new Config(DEFAULT_INSTANCE_NAME);
            Hazelcast.getOrCreateHazelcastInstance(config);

        } else if ("stop".equals(event.getType()) && "true".equals(shutdown)){
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName(DEFAULT_INSTANCE_NAME);
            instance.shutdown();
        }

    }
}
