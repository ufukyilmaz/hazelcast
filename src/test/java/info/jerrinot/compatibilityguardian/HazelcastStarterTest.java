package info.jerrinot.compatibilityguardian;

import org.junit.Test;

public class HazelcastStarterTest {

    @Test
    public void foo() throws InterruptedException {
//        HazelcastStarter.startHazelcastVersion("3.7.5");
//        HazelcastStarter.startHazelcastVersion("3.7.4");
        HazelcastStarter.startHazelcastVersion("3.7");

//        HazelcastStarter.startHazelcastClientVersion("3.7");
        HazelcastStarter.startHazelcastClientVersion("3.7.4");
        Thread.sleep(100000);
    }

}