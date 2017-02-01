package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class HazelcastStarterTest {

    @Test
    public void testMember() throws InterruptedException {
//        HazelcastStarter.startHazelcastVersion("3.7.5");
//        HazelcastStarter.startHazelcastVersion("3.7.4");
        HazelcastStarter.startHazelcastVersion("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting member " + version);
            HazelcastInstance instance = HazelcastStarter.startHazelcastVersion(version);
            System.out.println("Stopping member " + version);
            instance.shutdown();
        }
    }

    @Test
    public void testClientLifecycle() throws InterruptedException {
        HazelcastStarter.startHazelcastVersion("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastStarter.startHazelcastClientVersion(version);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }
    }

    @Test
    public void testClientMap() throws InterruptedException {
        HazelcastInstance memberInstance = HazelcastStarter.startHazelcastVersion("3.7");
        HazelcastInstance clientInstance = HazelcastStarter.startHazelcastClientVersion("3.7.2");

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

        clientMap.put(1, 2);

        assertEquals(2, (int)memberMap.get(1));
    }

}