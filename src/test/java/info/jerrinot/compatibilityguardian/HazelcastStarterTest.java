package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.*;

public class HazelcastStarterTest {

    @Test
    public void foo() throws InterruptedException {
//        HazelcastStarter.startHazelcastVersion("3.7.5");
//        HazelcastStarter.startHazelcastVersion("3.7.4");
        HazelcastInstance instance = HazelcastStarter.startHazelcastVersion("3.7");

        HazelcastInstance i = null;

        Thread.sleep(100000);
    }

}