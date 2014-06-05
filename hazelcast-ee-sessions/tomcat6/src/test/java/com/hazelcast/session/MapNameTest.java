package com.hazelcast.session;

import com.hazelcast.core.Hazelcast;
import org.junit.After;
import org.junit.Before;

/**
 * Created by mesutcelik on 6/4/14.
 */
public class MapNameTest extends Tomcat6MapTest {

     @Before
    public void setup() throws Exception{
        instance = Hazelcast.newHazelcastInstance();
         HazelcastSessionManager manager = new HazelcastSessionManager();
         manager.setClientOnly(true);
         manager.setMapName(AbstractMapNameTest.SESSION_REPLICATION_MAP_NAME);
        tomcat1 = createServer(TOMCAT_PORT_1,manager);
         manager = new HazelcastSessionManager();
         manager.setClientOnly(true);
         manager.setMapName(AbstractMapNameTest.SESSION_REPLICATION_MAP_NAME);
        tomcat2 = createServer(TOMCAT_PORT_2,manager);
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
