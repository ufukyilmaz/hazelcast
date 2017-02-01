package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
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
    public void testClient() throws InterruptedException {
        HazelcastStarter.startHazelcastVersion("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastStarter.startHazelcastClientVersion(version);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }
    }

}