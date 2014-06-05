package com.hazelcast.session;

import org.junit.After;
import org.junit.Before;

/**
 * Created by mesutcelik on 6/4/14.
 */
public class P2PNonStickyModeTest extends Tomcat6Test{

    @Before
    public void setup() throws Exception{
        HazelcastSessionManager manager = new HazelcastSessionManager();
        manager.setSticky(false);
        manager.setClientOnly(false);
        tomcat1 = createServer(TOMCAT_PORT_1, manager);
        tomcat2 = createServer(TOMCAT_PORT_2, manager);
        tomcat1.start();
        tomcat2.start();
    }

    @After
    public void tearDown() throws Exception{
        tomcat1.stop();
        tomcat2.stop();
    }

}
