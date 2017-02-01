package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class HazelcastStarterTest {

    @Test
    public void testMember() throws InterruptedException {
        HazelcastInstance alwaysRunningMember = HazelcastStarter.startHazelcastVersion("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting member " + version);
            HazelcastInstance instance = HazelcastStarter.startHazelcastVersion(version);
            System.out.println("Stopping member " + version);
            instance.shutdown();
        }

        alwaysRunningMember.shutdown();
    }

    @Test
    public void testClientLifecycle() throws InterruptedException {
        HazelcastInstance member = HazelcastStarter.startHazelcastVersion("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastStarter.startHazelcastClientVersion(version);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }

        member.shutdown();
    }

    @Test
    public void testClientMap() throws InterruptedException {
        HazelcastInstance memberInstance = HazelcastStarter.startHazelcastVersion("3.7");
        HazelcastInstance clientInstance = HazelcastStarter.startHazelcastClientVersion("3.7.2");

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

        clientMap.put(1, 2);

        assertEquals(2, (int)memberMap.get(1));

        clientInstance.shutdown();
        memberInstance.shutdown();
    }

    @Test
    public void testAdvancedClientMap() throws InterruptedException {
        HazelcastInstance memberInstance = HazelcastStarter.startHazelcastVersion("3.7");
        HazelcastInstance clientInstance = HazelcastStarter.startHazelcastClientVersion("3.7.2");

        System.out.println("About to terminate the client");
        clientInstance.getLifecycleService().terminate();
        System.out.println("Client terminated");

        memberInstance.shutdown();
    }

    @Test
    public void testClientMap_async() throws InterruptedException, ExecutionException {
        HazelcastInstance memberInstance = HazelcastStarter.startHazelcastVersion("3.7");
        HazelcastInstance clientInstance = HazelcastStarter.startHazelcastClientVersion("3.7.2");

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        clientMap.put(0, 1);
        ICompletableFuture<Integer> async = clientMap.getAsync(0);
        int value = async.get();

        assertEquals(1, value);

        clientInstance.shutdown();
        memberInstance.shutdown();
    }

}