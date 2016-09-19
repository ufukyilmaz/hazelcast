package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;

import static com.hazelcast.map.HDTestSupport.getEnterpriseMap;

public abstract class AbstractQueryCacheTestSupport extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IEnterpriseMap<Integer, Employee> map;
    protected Config config = new Config();
    protected String mapName = randomString();

    void prepare() {
    }

    @Before
    public void setUp() throws Exception {
        prepare();
        map = getMap();
    }

    private <K, V> IEnterpriseMap<K, V> getMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        instances = factory.newInstances(config);
        return getEnterpriseMap(instances[0], mapName);
    }

    protected void populateMap(IMap<Integer, Employee> map, int count) {
        populateMap(map, 0, count);
    }

    protected void populateMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.put(i, new Employee(i));
        }
    }

    protected void removeEntriesFromMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.remove(i);
        }
    }
}
