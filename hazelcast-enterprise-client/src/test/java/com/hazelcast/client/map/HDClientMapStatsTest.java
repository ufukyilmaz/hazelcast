package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import org.junit.After;
import org.junit.Before;

import static com.hazelcast.HDTestSupport.getHDConfig;

public class HDClientMapStatsTest extends ClientMapStatsTest {
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private String mapName = "mapName";
    private HazelcastInstance client;
    private HazelcastInstance member;

    @Before
    public void setUp() {
        member = factory.newHazelcastInstance(getHDConfig());
        client = factory.newHazelcastClient();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Override
    protected LocalMapStats getMapStats() {
        return member.getMap(mapName).getLocalMapStats();
    }

    @Override
    protected <K, V> IMap<K, V> getMap() {
        return client.getMap(mapName);
    }
}
