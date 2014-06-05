package com.hazelcast.session;

import com.hazelcast.core.Hazelcast;
import org.junit.After;
import org.junit.Before;

/**
 * Created by mesutcelik on 6/4/14.
 */
public class ClientStickyModeTest extends Tomcat7Test{

    @Before
    public void setup() throws Exception{
        Hazelcast.newHazelcastInstance();

        HazelcastSessionManager manager = new HazelcastSessionManager();
        manager.setSticky(true);
        manager.setClientOnly(true);
        tomcat1 = createServer(TOMCAT_PORT_1,manager);
        manager = new HazelcastSessionManager();
        manager.setSticky(true);
        manager.setClientOnly(true);
        tomcat2 = createServer(TOMCAT_PORT_2, manager);
        tomcat1.start();
        tomcat2.start();
    }

    @After
    public void tearDown() throws Exception{
        tomcat1.stop();
        tomcat2.stop();
        Hazelcast.shutdownAll();
    }

}
