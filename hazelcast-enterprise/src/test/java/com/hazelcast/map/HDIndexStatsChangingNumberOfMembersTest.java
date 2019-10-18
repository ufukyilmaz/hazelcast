package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexStatsChangingNumberOfMembersTest extends IndexStatsChangingNumberOfMembersTest {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.NATIVE}});
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Override
    @Test
    public void testIndexStatsQueryingChangingNumberOfMembers() {
        int queriesBulk = 100;

        int entryCount = 1000;
        final int lessEqualCount = 20;
        double expectedEqual = 1.0 - 1.0 / entryCount;
        double expectedGreaterEqual = 1.0 - ((double) lessEqualCount) / entryCount;

        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map1 = instance1.getMap(mapName);
        IMap<Integer, Integer> map2 = instance2.getMap(mapName);

        addIndex(map1);
        addIndex(map2);

        for (int i = 0; i < entryCount; ++i) {
            map1.put(i, i);
        }

        assertEquals(0, stats(map1).getQueryCount());
        assertEquals(0, stats(map1).getIndexedQueryCount());
        assertEquals(0, valueStats(map1).getQueryCount());
        assertEquals(0, stats(map2).getQueryCount());
        assertEquals(0, stats(map2).getIndexedQueryCount());
        assertEquals(0, valueStats(map2).getQueryCount());

        for (int i = 0; i < queriesBulk; i++) {
            map1.entrySet(Predicates.alwaysTrue());
            map1.entrySet(Predicates.equal("this", 10));
            map2.entrySet(Predicates.lessEqual("this", lessEqualCount));
        }

        assertEquals(3 * queriesBulk, stats(map1).getQueryCount());
        assertEquals(2 * queriesBulk, stats(map1).getIndexedQueryCount());
        assertEquals(2 * queriesBulk, valueStats(map1).getQueryCount());
        assertEquals(3 * queriesBulk, stats(map2).getQueryCount());
        assertEquals(2 * queriesBulk, stats(map2).getIndexedQueryCount());
        assertEquals(2 * queriesBulk, valueStats(map2).getQueryCount());

        double originalOverallAverageHitSelectivity = calculateOverallSelectivity(map1, map2);
        assertEquals((expectedEqual + expectedGreaterEqual) / 2, originalOverallAverageHitSelectivity, 0.015);

        long originalMap1QueryCount = stats(map1).getQueryCount();
        long originalMap1IndexedQueryCount = stats(map1).getIndexedQueryCount();
        long originalMap1IndexQueryCount = valueStats(map1).getQueryCount();
        long originalMap2QueryCount = stats(map2).getQueryCount();
        long originalMap2IndexedQueryCount = stats(map2).getIndexedQueryCount();
        long originalMap2IndexQueryCount = valueStats(map2).getQueryCount();

        // let's add another member
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map3 = instance3.getMap(mapName);
        addIndex(map3);

        waitAllForSafeState(instance1, instance2, instance3);

        // check that local stats were not affected by adding new member to cluster
        assertEquals(originalMap1QueryCount, stats(map1).getQueryCount());
        assertEquals(originalMap1IndexedQueryCount, stats(map1).getIndexedQueryCount());
        assertEquals(originalMap1IndexQueryCount, valueStats(map1).getQueryCount());
        assertEquals(originalMap2QueryCount, stats(map2).getQueryCount());
        assertEquals(originalMap2IndexedQueryCount, stats(map2).getIndexedQueryCount());
        assertEquals(originalMap2IndexQueryCount, valueStats(map2).getQueryCount());

        for (int i = 0; i < queriesBulk; i++) {
            map1.entrySet(Predicates.alwaysTrue());
            map3.entrySet(Predicates.equal("this", 10));
            map2.entrySet(Predicates.lessEqual("this", lessEqualCount));
        }

        assertEquals(6 * queriesBulk, stats(map1).getQueryCount());
        assertEquals(4 * queriesBulk, stats(map1).getIndexedQueryCount());
        assertEquals(4 * queriesBulk, valueStats(map1).getQueryCount());
        assertEquals(6 * queriesBulk, stats(map2).getQueryCount());
        assertEquals(4 * queriesBulk, stats(map2).getIndexedQueryCount());
        assertEquals(4 * queriesBulk, valueStats(map2).getQueryCount());
        assertEquals(3 * queriesBulk, stats(map3).getQueryCount());
        assertEquals(2 * queriesBulk, stats(map3).getIndexedQueryCount());
        assertEquals(2 * queriesBulk, valueStats(map3).getQueryCount());

        originalOverallAverageHitSelectivity = calculateOverallSelectivity(map1, map2, map3);
        assertEquals((expectedEqual + expectedGreaterEqual) / 2, originalOverallAverageHitSelectivity, 0.015);

        originalMap1QueryCount = stats(map1).getQueryCount();
        originalMap1IndexedQueryCount = stats(map1).getIndexedQueryCount();
        originalMap1IndexQueryCount = valueStats(map1).getQueryCount();
        long originalMap1AverageHitLatency = valueStats(map1).getAverageHitLatency();
        double originalMap1AverageHitSelectivity = valueStats(map1).getAverageHitSelectivity();
        long originalMap3QueryCount = stats(map3).getQueryCount();
        long originalMap3IndexedQueryCount = stats(map3).getIndexedQueryCount();
        long originalMap3IndexQueryCount = valueStats(map3).getQueryCount();
        long originalMap3AverageHitLatency = valueStats(map3).getAverageHitLatency();
        double originalMap3AverageHitSelectivity = valueStats(map3).getAverageHitSelectivity();

        // After removing member AverageHitSelectivity will not provide accurate value => this serves just for ensure
        // that AverageHitSelectivity is still counted correctly on live members.
        long map2Hits = valueStats(map2).getHitCount();
        double map2TotalHitSelectivity = valueStats(map2).getAverageHitSelectivity() * map2Hits;

        // let's remove one member
        instance2.shutdown();
        waitAllForSafeState(instance1, instance3);

        // check that local stats were not affected by removing member from cluster
        assertEquals(originalMap1QueryCount, stats(map1).getQueryCount());
        assertEquals(originalMap1IndexedQueryCount, stats(map1).getIndexedQueryCount());
        assertEquals(originalMap1IndexQueryCount, valueStats(map1).getQueryCount());
        assertEquals(originalMap1AverageHitLatency, valueStats(map1).getAverageHitLatency());
        assertEquals(originalMap1AverageHitSelectivity, valueStats(map1).getAverageHitSelectivity(), 0.001);
        assertEquals(originalMap3QueryCount, stats(map3).getQueryCount());
        assertEquals(originalMap3IndexedQueryCount, stats(map3).getIndexedQueryCount());
        assertEquals(originalMap3IndexQueryCount, valueStats(map3).getQueryCount());
        assertEquals(originalMap3AverageHitLatency, valueStats(map3).getAverageHitLatency());
        assertEquals(originalMap3AverageHitSelectivity, valueStats(map3).getAverageHitSelectivity(), 0.001);

        assertEquals(originalOverallAverageHitSelectivity,
                calculateOverallSelectivity(map2Hits, map2TotalHitSelectivity, map1, map3), 0.015);

        for (int i = 0; i < queriesBulk; i++) {
            map3.entrySet(Predicates.alwaysTrue());
            map1.entrySet(Predicates.equal("this", 10));
            map3.entrySet(Predicates.lessEqual("this", lessEqualCount));
        }

        assertEquals(9 * queriesBulk, stats(map1).getQueryCount());
        assertEquals(6 * queriesBulk, stats(map1).getIndexedQueryCount());
        assertEquals(6 * queriesBulk, valueStats(map1).getQueryCount());
        assertEquals(6 * queriesBulk, stats(map3).getQueryCount());
        assertEquals(4 * queriesBulk, stats(map3).getIndexedQueryCount());
        assertEquals(4 * queriesBulk, valueStats(map3).getQueryCount());

        // This work correctly only due to we stored data from shutdown member and uses this data for counting
        // originalOverallAverageHitSelectivity. However this not represent real scenario. This check is here just for ensure
        // that AverageHitSelectivity is still counted correctly on live members.
        originalOverallAverageHitSelectivity = calculateOverallSelectivity(map2Hits, map2TotalHitSelectivity, map1, map3);
        assertEquals((expectedEqual + expectedGreaterEqual) / 2, originalOverallAverageHitSelectivity, 0.015);
    }

    @Override
    @Test
    public void testIndexStatsOperationChangingNumberOfMembers() {
        int inserts = 100;
        int updates = 20;
        int removes = 20;

        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map1 = instance1.getMap(mapName);
        IMap<Integer, Integer> map2 = instance2.getMap(mapName);

        addIndex(map1);
        addIndex(map2);

        assertEquals(0, valueStats(map1).getInsertCount());
        assertEquals(0, valueStats(map1).getUpdateCount());
        assertEquals(0, valueStats(map1).getRemoveCount());
        assertEquals(0, valueStats(map1).getTotalInsertLatency());
        assertEquals(0, valueStats(map1).getTotalRemoveLatency());
        assertEquals(0, valueStats(map1).getTotalUpdateLatency());
        assertEquals(0, valueStats(map2).getInsertCount());
        assertEquals(0, valueStats(map2).getUpdateCount());
        assertEquals(0, valueStats(map2).getRemoveCount());
        assertEquals(0, valueStats(map2).getTotalInsertLatency());
        assertEquals(0, valueStats(map2).getTotalRemoveLatency());
        assertEquals(0, valueStats(map2).getTotalUpdateLatency());

        for (int i = 0; i < inserts; ++i) {
            map1.put(i, i);
        }
        for (int i = 0; i < updates; ++i) {
            map1.put(i, i * i);
            map2.put(i + updates, i * i);
        }
        for (int i = inserts - removes; i < inserts; ++i) {
            map2.remove(i);
        }

        assertEquals(inserts, valueStats(map1).getInsertCount() + valueStats(map2).getInsertCount());
        assertEquals(2 * updates, valueStats(map1).getUpdateCount() + valueStats(map2).getUpdateCount());
        assertEquals(removes, valueStats(map1).getRemoveCount() + valueStats(map2).getRemoveCount());

        assertTrue(valueStats(map1).getTotalInsertLatency() > 0);
        assertTrue(valueStats(map1).getTotalRemoveLatency() > 0);
        assertTrue(valueStats(map1).getTotalUpdateLatency() > 0);
        assertTrue(valueStats(map2).getTotalInsertLatency() > 0);
        assertTrue(valueStats(map2).getTotalRemoveLatency() > 0);
        assertTrue(valueStats(map2).getTotalUpdateLatency() > 0);

        long originalMap1InsertCount = valueStats(map1).getInsertCount();
        long originalMap1UpdateCount = valueStats(map1).getUpdateCount();
        long originalMap1RemoveCount = valueStats(map1).getRemoveCount();
        long originalMap2InsertCount = valueStats(map2).getInsertCount();
        long originalMap2UpdateCount = valueStats(map2).getUpdateCount();
        long originalMap2RemoveCount = valueStats(map2).getRemoveCount();

        // let's add another member
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map3 = instance3.getMap(mapName);
        addIndex(map3);

        waitAllForSafeState(instance1, instance2, instance3);

        assertChange("insertCount", originalMap1InsertCount + originalMap2InsertCount,
                valueStats(map1).getInsertCount() + valueStats(map2).getInsertCount());
        assertChange("updateCount", originalMap1UpdateCount + originalMap2UpdateCount,
                valueStats(map1).getUpdateCount() + valueStats(map2).getUpdateCount());
        assertChange("removeCount", originalMap1RemoveCount + originalMap2RemoveCount,
                valueStats(map1).getRemoveCount() + valueStats(map2).getRemoveCount());

        long originalTotalInsertCount =
                valueStats(map1).getInsertCount() + valueStats(map2).getInsertCount() + valueStats(map3).getInsertCount();
        long originalTotalUpdateCount =
                valueStats(map1).getUpdateCount() + valueStats(map2).getUpdateCount() + valueStats(map3).getUpdateCount();
        long originalTotalRemoveCount =
                valueStats(map1).getRemoveCount() + valueStats(map2).getRemoveCount() + valueStats(map3).getRemoveCount();
        for (int i = inserts; i < 2 * inserts; ++i) {
            map3.put(i, i);
        }
        for (int i = inserts; i < inserts + updates; ++i) {
            map2.put(i, i * i);
            map3.put(i + updates, i * i);
        }
        for (int i = 2 * inserts - updates; i < 2 * inserts; ++i) {
            map1.remove(i);
        }

        assertEquals(originalTotalInsertCount + inserts,
                valueStats(map1).getInsertCount() + valueStats(map2).getInsertCount() + valueStats(map3).getInsertCount());
        assertEquals(originalTotalUpdateCount + 2 * updates,
                valueStats(map1).getUpdateCount() + valueStats(map2).getUpdateCount() + valueStats(map3).getUpdateCount());
        assertEquals(originalTotalRemoveCount + removes,
                valueStats(map1).getRemoveCount() + valueStats(map2).getRemoveCount() + valueStats(map3).getRemoveCount());

        originalMap1InsertCount = valueStats(map1).getInsertCount();
        originalMap1UpdateCount = valueStats(map1).getUpdateCount();
        originalMap1RemoveCount = valueStats(map1).getRemoveCount();
        long originalMap1TotalInsertLatency = valueStats(map1).getTotalInsertLatency();
        long originalMap1TotalRemoveLatency = valueStats(map1).getTotalRemoveLatency();
        long originalMap1TotalUpdateLatency = valueStats(map1).getTotalUpdateLatency();
        long originalMap3InsertCount = valueStats(map3).getInsertCount();
        long originalMap3UpdateCount = valueStats(map3).getUpdateCount();
        long originalMap3RemoveCount = valueStats(map3).getRemoveCount();
        long originalMap3TotalInsertLatency = valueStats(map3).getTotalInsertLatency();
        long originalMap3TotalRemoveLatency = valueStats(map3).getTotalRemoveLatency();
        long originalMap3TotalUpdateLatency = valueStats(map3).getTotalUpdateLatency();

        // let's remove one member
        instance2.shutdown();
        waitAllForSafeState(instance1, instance3);

        assertEquals(originalMap1InsertCount, valueStats(map1).getInsertCount());
        assertEquals(originalMap1UpdateCount, valueStats(map1).getUpdateCount());
        assertEquals(originalMap1RemoveCount, valueStats(map1).getRemoveCount());
        assertEquals(originalMap1TotalInsertLatency, valueStats(map1).getTotalInsertLatency());
        assertEquals(originalMap1TotalRemoveLatency, valueStats(map1).getTotalRemoveLatency());
        assertEquals(originalMap1TotalUpdateLatency, valueStats(map1).getTotalUpdateLatency());
        assertEquals(originalMap3InsertCount, valueStats(map3).getInsertCount());
        assertEquals(originalMap3UpdateCount, valueStats(map3).getUpdateCount());
        assertEquals(originalMap3RemoveCount, valueStats(map3).getRemoveCount());
        assertEquals(originalMap3TotalInsertLatency, valueStats(map3).getTotalInsertLatency());
        assertEquals(originalMap3TotalRemoveLatency, valueStats(map3).getTotalRemoveLatency());
        assertEquals(originalMap3TotalUpdateLatency, valueStats(map3).getTotalUpdateLatency());

        long originalMap1Map3InsertCount = valueStats(map1).getInsertCount() + valueStats(map3).getInsertCount();
        long originalMap1Map3UpdateCount = valueStats(map1).getUpdateCount() + valueStats(map3).getUpdateCount();
        long originalMap1Map3RemoveCount = valueStats(map1).getRemoveCount() + valueStats(map3).getRemoveCount();

        for (int i = 2 * inserts; i < 3 * inserts; ++i) {
            map3.put(i, i);
        }
        for (int i = 2 * inserts; i < 2 * inserts + updates; ++i) {
            map3.put(i, i * i);
            map1.put(i + updates, i * i);
        }
        for (int i = 3 * inserts - updates; i < 3 * inserts; ++i) {
            map3.remove(i);
        }

        assertEquals(originalMap1Map3InsertCount + inserts,
                valueStats(map1).getInsertCount() + valueStats(map3).getInsertCount());
        assertEquals(originalMap1Map3UpdateCount + 2 * updates,
                valueStats(map1).getUpdateCount() + valueStats(map3).getUpdateCount());
        assertEquals(originalMap1Map3RemoveCount + removes,
                valueStats(map1).getRemoveCount() + valueStats(map3).getRemoveCount());
    }

    private static void assertChange(String label, long expected, long actual) {
        // we are adding a new member, maximum amount of stats "data" we can lose is one half
        assertBetween(label, actual, (long) (expected - expected / 2.0), expected);
    }

}