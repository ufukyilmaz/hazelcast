package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.enterprise.wan.impl.replication.DefaultWanBatchSender;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.internal.management.ManagementCenterEventListener;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.WanConsistencyCheckFinishedEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckStartedEvent;
import com.hazelcast.internal.management.events.WanFullSyncFinishedEvent;
import com.hazelcast.internal.management.events.WanMerkleSyncFinishedEvent;
import com.hazelcast.internal.management.events.WanSyncProgressUpdateEvent;
import com.hazelcast.internal.management.events.WanSyncStartedEvent;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.WanSyncStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.wan.WanPublisherState.STOPPED;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationPublisher;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanSyncTrackingTest extends HazelcastTestSupport {

    private static final String MAP1_NAME = "map1";
    private static final String MAP2_NAME = "map2";
    private static final String REPLICATION_NAME = "wanReplication";
    private static final int TOTAL_ENTRIES = 1000;

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private WanReplication wanReplication;

    @Parameter(0)
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Parameter(1)
    public int maxConcurrentInvocations;

    @Parameters(name = "consistencyCheckStrategy:{0}, maxConcurrentInvocations:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE, -1},
                {NONE, 100},
                {MERKLE_TREES, -1},
                {MERKLE_TREES, 100}
        });
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2).setup();
        targetCluster = clusterB(factory, 1).setup();

        configureMerkleTrees(sourceCluster);
        configureMerkleTrees(targetCluster);

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withInitialPublisherState(STOPPED)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .withReplicationBatchSize(100)
                .withWanPublisher(WanBatchSyncingPublisher.class)
                .withWanBatchSender(SyncingWanBatchSender.class)
                .withMaxConcurrentInvocations(maxConcurrentInvocations)
                .setup();

        sourceCluster.replicateMap(MAP1_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        sourceCluster.replicateMap(MAP2_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();
    }

    private void configureMerkleTrees(Cluster cluster) {
        if (consistencyCheckStrategy == MERKLE_TREES) {
            cluster.getConfig()
                    .getMapConfig(MAP1_NAME)
                    .getMerkleTreeConfig()
                    .setEnabled(true)
                    .setDepth(6);

            cluster.getConfig()
                    .getMapConfig(MAP2_NAME)
                    .getMerkleTreeConfig()
                    .setEnabled(true)
                    .setDepth(6);
        }
    }

    @Test
    public void testWithOneMap() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(400, MAP1_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, TOTAL_ENTRIES);

        sourceCluster.syncMap(wanReplication, MAP1_NAME);

        // first, we verify the sync statistics and MC interaction with suspended synchronization
        verifyOngoingSync(testContext);

        // then we let the synchronization to finish and verify the stats and MC interaction accordingly
        verifyFinishedSync(testContext);
    }

    @Test
    public void testWithOneEmptyMap() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(Integer.MAX_VALUE, 0, MAP1_NAME);

        sourceCluster.syncMap(wanReplication, MAP1_NAME);
        verifyFinishedSyncEmpty(testContext);
    }

    @Test
    public void testWithTwoEmptyMaps() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(Integer.MAX_VALUE, 0, MAP1_NAME, MAP2_NAME);

        // creating the maps for syncAll
        fillMap(sourceCluster, MAP1_NAME, 0, 1);
        fillMap(sourceCluster, MAP2_NAME, 0, 1);
        sourceCluster.getAMember().getMap(MAP1_NAME).clear();
        sourceCluster.getAMember().getMap(MAP2_NAME).clear();

        sourceCluster.syncAllMaps(wanReplication);
        verifyFinishedSyncEmpty(testContext);
    }

    @Test
    public void testWithOneEntryInOneMap() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(Integer.MAX_VALUE, 1, MAP1_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, 1);

        sourceCluster.syncMap(wanReplication, MAP1_NAME);
        verifyFinishedSyncEmpty(testContext);
    }

    @Test
    public void testWithOneEntryInBothOfTwoMaps() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(Integer.MAX_VALUE, 1, MAP1_NAME, MAP2_NAME);

        // creating the maps for syncAll
        fillMap(sourceCluster, MAP1_NAME, 0, 1);
        fillMap(sourceCluster, MAP2_NAME, 0, 1);

        sourceCluster.syncAllMaps(wanReplication);
        verifyFinishedSyncEmpty(testContext);
    }

    @Test
    public void testWithAllMaps() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(Integer.MAX_VALUE, MAP1_NAME, MAP2_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, TOTAL_ENTRIES);
        fillMap(sourceCluster, MAP2_NAME, 0, TOTAL_ENTRIES);

        sourceCluster.syncAllMaps(wanReplication);
        verifyFinishedSync(testContext);
    }

    @Test
    public void testWithOneMapTwoSyncs() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(400, MAP1_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, TOTAL_ENTRIES);

        sourceCluster.syncMap(wanReplication, MAP1_NAME);

        // first, we verify the sync statistics and MC interaction with suspended synchronization
        verifyOngoingSync(testContext);

        // then we let the synchronization to finish and verify the stats and MC interaction accordingly
        verifyFinishedSync(testContext);

        // second SYNC after the first finished
        targetCluster.getAMember().getMap(MAP1_NAME).clear();
        testContext = setupTestContext(400, MAP1_NAME);

        sourceCluster.syncMap(wanReplication, MAP1_NAME);

        // first, we verify the sync statistics and MC interaction with suspended synchronization
        verifyOngoingSync(testContext);

        // then we let the synchronization to finish and verify the stats and MC interaction accordingly
        verifyFinishedSync(testContext);
    }

    @Test
    public void testWithAllMapsTwoSyncs() {
        sourceCluster.startClusterAndWaitForSafeState();
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(MAP1_NAME, MAP2_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, TOTAL_ENTRIES);
        fillMap(sourceCluster, MAP2_NAME, 0, TOTAL_ENTRIES);

        sourceCluster.syncAllMaps(wanReplication);

        verifyFinishedSync(testContext);

        // second SYNC after the first finished
        targetCluster.getAMember().getMap(MAP1_NAME).clear();
        targetCluster.getAMember().getMap(MAP2_NAME).clear();
        testContext = setupTestContext(MAP1_NAME, MAP2_NAME);

        sourceCluster.syncAllMaps(wanReplication);

        verifyFinishedSync(testContext);
    }

    @Test
    public void testWithOneMapOverlappingSyncs() {
        sourceCluster.startClusterAndWaitForSafeState();
        HazelcastInstance sourceMemberOne = sourceCluster.getMembers()[0];
        HazelcastInstance sourceMemberTwo = sourceCluster.getMembers()[1];
        targetCluster.startCluster();

        TestContext testContext = setupTestContext(400, MAP1_NAME);

        fillMap(sourceCluster, MAP1_NAME, 0, TOTAL_ENTRIES);

        sourceCluster.syncMapOnMember(wanReplication, MAP1_NAME, sourceMemberOne);

        // we wait until the verification point and then trigger another overlapping synchronization
        assertOpenEventually(testContext.syncSuspendedLatch);
        CountDownLatch syncResumeLatch = testContext.syncResumeLatch;
        extractLastSyncResults(testContext);

        targetCluster.getAMember().getMap(MAP1_NAME).clear();
        sourceCluster.syncMapOnMember(wanReplication, MAP1_NAME, sourceMemberTwo);
        // after triggering the overlapping synchronization, we resume synchronization
        syncResumeLatch.countDown();

        verifyFinishedSyncOverlapped(testContext);
    }

    private TestContext setupTestContext(String... mapNames) {
        int neverSuspendSync = Integer.MAX_VALUE;
        int nonEmptyPartitions = getNode(sourceCluster.getAMember()).getPartitionService().getPartitionCount();
        return setupTestContext(neverSuspendSync, nonEmptyPartitions, mapNames);
    }

    private TestContext setupTestContext(int suspendSyncAfterRecords, String... mapNames) {
        int nonEmptyPartitions = getNode(sourceCluster.getAMember()).getPartitionService().getPartitionCount();
        return setupTestContext(suspendSyncAfterRecords, nonEmptyPartitions, mapNames);
    }

    private TestContext setupTestContext(int suspendSyncAfterRecords, int nonEmptyPartitions, String... mapNames) {
        TestContext testContext = new TestContext(mapNames, nonEmptyPartitions);
        sourceCluster.forEachMember(member -> {
            WanBatchSyncingPublisher wanBatchReplication = (WanBatchSyncingPublisher) wanReplicationPublisher(member,
                    wanReplication);
            wanBatchReplication.setup(suspendSyncAfterRecords, testContext);
            testContext.mcEventListenerMap.putIfAbsent(member, mock(ManagementCenterEventListener.class));
        });

        registerMcEventListener(testContext);

        return testContext;
    }

    private void extractLastSyncResults(TestContext testContext) {
        sourceCluster.forEachMember(member -> {
            for (Map.Entry<String, WanSyncStats> entry : getLastSyncResult(member).entrySet()) {
                String mapName = entry.getKey();
                WanSyncStats syncStats = entry.getValue();
                SyncKey syncKey = new SyncKey(member, syncStats.getUuid(), mapName);
                testContext.syncStatsMap.put(syncKey, syncStats);
            }
        });
    }

    private void verifyOngoingSync(TestContext testContext) {
        assertOpenEventually(testContext.syncSuspendedLatch);

        if (maxConcurrentInvocations == -1) {
            assertTrueEventually(() -> {
                extractLastSyncResults(testContext);
                verifyRecordsInSyncStat(testContext);
                verifyManagementCenterInteractionSyncOnGoing(testContext);
            });
        }

        testContext.syncResumeLatch.countDown();
    }

    private void verifyFinishedSyncEmpty(TestContext testContext) {
        verifyMapReplicated(sourceCluster, targetCluster, MAP1_NAME);
        verifyMapReplicated(sourceCluster, targetCluster, MAP2_NAME);

        assertTrueEventually(() -> {
            extractLastSyncResults(testContext);
            verifyRecordsInSyncStat(testContext);
            verifyAllPartitionsSynced(testContext);
            verifyManagementCenterInteractionSyncCompleted(testContext);
        });
    }

    private void verifyFinishedSync(TestContext testContext) {
        verifyMapReplicated(sourceCluster, targetCluster, MAP1_NAME);
        verifyMapReplicated(sourceCluster, targetCluster, MAP2_NAME);

        assertTrueEventually(() -> {
            extractLastSyncResults(testContext);
            verifyRecordsInSyncStat(testContext);
            verifyAllPartitionsSynced(testContext);
            verifyManagementCenterInteractionSyncCompleted(testContext);
        });
    }

    private void verifyFinishedSyncOverlapped(TestContext testContext) {
        verifyMapReplicated(sourceCluster, targetCluster, MAP1_NAME);
        verifyMapReplicated(sourceCluster, targetCluster, MAP2_NAME);

        assertTrueEventually(() -> {
            extractLastSyncResults(testContext);
            verifyRecordsInSyncStat(testContext);
            verifyAllPartitionsSyncedOverlapped(testContext);
            verifyManagementCenterInteractionOverlapped(testContext);
        });
    }

    private void verifyRecordsInSyncStat(TestContext testContext) {
        int expectedRecordsSynced = testContext.sentCount.get();
        AtomicInteger totalSyncedRecords = new AtomicInteger();
        testContext.syncStatsMap.values().forEach(syncStats -> totalSyncedRecords.addAndGet(syncStats.getRecordsSynced()));

        assertEquals(String.format("Total synced records should be %d, but it is %d", expectedRecordsSynced,
                totalSyncedRecords.get()), expectedRecordsSynced, totalSyncedRecords.get());
    }

    private void verifyAllPartitionsSynced(TestContext testContext) {
        int syncedMaps = testContext.mapNames.length;
        // merkle sync synchronizes only the partitions that have differences
        int expectedPartitionsSynced = syncedMaps * (consistencyCheckStrategy == NONE ? testContext.totalPartitions
                : testContext.nonEmptyPartitions);
        Map<UUID, AtomicInteger> partitionsSyncedMap = new HashMap<>();

        testContext.syncStatsMap.values().forEach(syncStats -> {
            AtomicInteger counter = partitionsSyncedMap.computeIfAbsent(syncStats.getUuid(), k -> new AtomicInteger());
            counter.addAndGet(syncStats.getPartitionsSynced());
        });

        for (Map.Entry<UUID, AtomicInteger> entry : partitionsSyncedMap.entrySet()) {
            UUID uuid = entry.getKey();
            int actualPartitionsSynced = entry.getValue().get();
            assertEquals(String.format("Total synced partitions for %s should be %d, but it is %d", uuid,
                    expectedPartitionsSynced, actualPartitionsSynced), expectedPartitionsSynced, actualPartitionsSynced);
        }
    }

    private void verifyAllPartitionsSyncedOverlapped(TestContext testContext) {
        int syncedMaps = testContext.mapNames.length;
        // merkle sync synchronizes only the partitions that have differences
        int expectedPartitionsSynced = syncedMaps * (consistencyCheckStrategy == NONE ? testContext.totalPartitions
                : testContext.nonEmptyPartitions);
        Map<UUID, AtomicInteger> partitionsSyncedMap = new HashMap<>();

        testContext.syncStatsMap.values().forEach(syncStats -> {
            AtomicInteger counter = partitionsSyncedMap.computeIfAbsent(syncStats.getUuid(), k -> new AtomicInteger());
            counter.addAndGet(syncStats.getPartitionsSynced());
        });

        for (Map.Entry<UUID, AtomicInteger> entry : partitionsSyncedMap.entrySet()) {
            UUID uuid = entry.getKey();
            int actualPartitionsSynced = entry.getValue().get();
            if (consistencyCheckStrategy == MERKLE_TREES) {
                // if two merkle synchronization requests overlap, we don't know how many
                // partitions will be actually synchronized
                // syncs can overlap in sync phase or in consistency check phase
                assertTrue(String.format("Total synced partitions for %s should be less or equal than %d, but it is %d", uuid,
                        expectedPartitionsSynced, actualPartitionsSynced), actualPartitionsSynced <= expectedPartitionsSynced);
            } else {
                assertEquals(String.format("Total synced partitions for %s should be %d, but it is %d", uuid,
                        expectedPartitionsSynced, actualPartitionsSynced), expectedPartitionsSynced, actualPartitionsSynced);
            }
        }
    }

    private void verifyManagementCenterInteractionSyncOnGoing(TestContext testContext) {
        AtomicInteger totalRecordsReported = new AtomicInteger();
        AtomicInteger totalUpdateEvents = new AtomicInteger();
        int syncedMaps = testContext.mapNames.length;

        Map<SyncKey, Integer> recordsSyncedMap = new HashMap<>();

        testContext.mcEventListenerMap.keySet().forEach(instance -> {
            ManagementCenterEventListener mcEventListener = testContext.mcEventListenerMap.get(instance);
            ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);

            verify(mcEventListener, atLeastOnce()).onEventLogged(eventCaptor.capture());

            processUpdateEvents(recordsSyncedMap, instance, eventCaptor,
                    (key, updateEvent) -> totalUpdateEvents.getAndIncrement());

            Event lastEvent = eventCaptor.getValue();
            if (consistencyCheckStrategy == NONE) {
                assertInstanceOf(WanSyncProgressUpdateEvent.class, lastEvent);

                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, never()).onEventLogged(any(WanFullSyncFinishedEvent.class));
            } else {
                assertInstanceOf(WanSyncProgressUpdateEvent.class, lastEvent);

                verify(mcEventListener, atLeastOnce()).onEventLogged(any(WanConsistencyCheckStartedEvent.class));
                verify(mcEventListener, atMost(syncedMaps)).onEventLogged(any(WanConsistencyCheckStartedEvent.class));
                verify(mcEventListener, atLeastOnce()).onEventLogged(any(WanConsistencyCheckFinishedEvent.class));
                verify(mcEventListener, atMost(syncedMaps)).onEventLogged(any(WanConsistencyCheckFinishedEvent.class));
                verify(mcEventListener, atLeastOnce()).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, atMost(syncedMaps)).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, never()).onEventLogged(any(WanMerkleSyncFinishedEvent.class));
            }
        });

        recordsSyncedMap.values().forEach(totalRecordsReported::addAndGet);

        int sentCount = testContext.sentCount.get();
        int reportedCount = totalRecordsReported.get();

        assertTrue(reportedCount > 0);
        // we can't assert on a concrete number since we report to MC only when a partition is fully synchronized
        assertTrue(String.format("No more than the sent count should be reported to MC. Sent count: %d, reported to MC: %d",
                sentCount, reportedCount), sentCount >= reportedCount);
    }

    private void verifyManagementCenterInteractionSyncCompleted(TestContext testContext) {
        AtomicInteger totalRecordsReported = new AtomicInteger();
        AtomicInteger totalUpdateEvents = new AtomicInteger();
        int syncedMaps = testContext.mapNames.length;
        Map<SyncKey, Integer> recordsSyncedMap = new HashMap<>();
        Map<SyncKey, List<WanSyncProgressUpdateEvent>> capturedProgressUpdatesMap = new HashMap<>();

        testContext.mcEventListenerMap.keySet().forEach(instance -> {
            ManagementCenterEventListener mcEventListener = testContext.mcEventListenerMap.get(instance);
            ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);

            verify(mcEventListener, atLeastOnce()).onEventLogged(eventCaptor.capture());

            processUpdateEvents(recordsSyncedMap, instance, eventCaptor, (key, updateEvent) -> {
                capturedProgressUpdatesMap.computeIfAbsent(key, k -> new LinkedList<>()).add(updateEvent);
                totalUpdateEvents.getAndIncrement();
            });

            Event lastEvent = eventCaptor.getValue();

            if (consistencyCheckStrategy == NONE) {
                assertInstanceOf(WanFullSyncFinishedEvent.class, lastEvent);

                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanFullSyncFinishedEvent.class));
            } else {
                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanConsistencyCheckStartedEvent.class));
                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanConsistencyCheckFinishedEvent.class));
                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, times(syncedMaps)).onEventLogged(any(WanMerkleSyncFinishedEvent.class));
            }
        });

        // verify that the sent and reported record counts match
        recordsSyncedMap.values().forEach(totalRecordsReported::addAndGet);
        int sentCount = testContext.sentCount.get();
        int reportedCount = totalRecordsReported.get();
        assertEquals(sentCount, reportedCount);

        // verify the total number of update events
        final int expectedUpdateEvents;
        if (consistencyCheckStrategy == NONE) {
            // with full sync we expect all partitions are synced
            expectedUpdateEvents = syncedMaps * testContext.totalPartitions;
        } else {
            // with merkle sync we expect that only the inconsistent partitions
            // are synced, which in this test are the non-empty ones
            expectedUpdateEvents = syncedMaps * testContext.nonEmptyPartitions;
        }
        assertEquals(expectedUpdateEvents, totalUpdateEvents.get());

        // verify synced partitions increases by one in every progress update event
        MutableInteger updateEventsVerified = new MutableInteger();
        capturedProgressUpdatesMap.values().forEach(updateEvents -> {
            int lastPartitionsSynced = 0;
            for (WanSyncProgressUpdateEvent updateEvent : updateEvents) {
                assertEquals(lastPartitionsSynced + 1, updateEvent.getPartitionsSynced());
                lastPartitionsSynced = updateEvent.getPartitionsSynced();
                updateEventsVerified.getAndInc();
            }
        });
        assertEquals(expectedUpdateEvents, updateEventsVerified.value);
    }

    private void verifyManagementCenterInteractionOverlapped(TestContext testContext) {
        AtomicInteger totalRecordsReported = new AtomicInteger();
        AtomicInteger totalUpdateEvents = new AtomicInteger();
        int syncedMaps = testContext.mapNames.length;
        int maxInvocations = syncedMaps * sourceCluster.size();
        Map<SyncKey, Integer> recordsSyncedMap = new HashMap<>();

        testContext.mcEventListenerMap.keySet().forEach(instance -> {
            ManagementCenterEventListener mcEventListener = testContext.mcEventListenerMap.get(instance);
            ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);

            verify(mcEventListener, atLeastOnce()).onEventLogged(eventCaptor.capture());

            processUpdateEvents(recordsSyncedMap, instance, eventCaptor,
                    (key, updateEvent) -> totalUpdateEvents.getAndIncrement());

            Event lastEvent = eventCaptor.getValue();
            if (consistencyCheckStrategy == NONE) {
                assertInstanceOf(WanFullSyncFinishedEvent.class, lastEvent);
                verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanSyncStartedEvent.class));
                verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanFullSyncFinishedEvent.class));
            } else {
                verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanConsistencyCheckStartedEvent.class));
                verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanConsistencyCheckFinishedEvent.class));

                // in the overlapping with concurrent invocations test case it is
                // possible that the consistency check of the overlapping sync
                // request doesn't see any difference if the previous sync is
                // still on the fly when the overlapping one is requested
                if (consistencyCheckStrategy == MERKLE_TREES && maxConcurrentInvocations == 100) {
                    assertTrue(lastEvent instanceof WanMerkleSyncFinishedEvent
                            || lastEvent instanceof WanConsistencyCheckFinishedEvent);
                    verify(mcEventListener, atMost(maxInvocations)).onEventLogged(any(WanSyncStartedEvent.class));
                    verify(mcEventListener, atMost(maxInvocations)).onEventLogged(any(WanMerkleSyncFinishedEvent.class));
                } else {
                    assertInstanceOf(WanMerkleSyncFinishedEvent.class, lastEvent);
                    verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanSyncStartedEvent.class));
                    verify(mcEventListener, times(maxInvocations)).onEventLogged(any(WanMerkleSyncFinishedEvent.class));
                }
            }
        });

        recordsSyncedMap.values().forEach(totalRecordsReported::addAndGet);

        int sentCount = testContext.sentCount.get();
        int reportedCount = totalRecordsReported.get();
        assertEquals(sentCount, reportedCount);
    }

    private void processUpdateEvents(Map<SyncKey, Integer> recordsSyncedMap, HazelcastInstance instance,
                                     ArgumentCaptor<Event> eventCaptor,
                                     BiConsumer<SyncKey, WanSyncProgressUpdateEvent> consumer) {
        // we need to work around a Mockito limitation here
        // ArgumentCaptor currently does not offer a feature for capturing arguments with a filter
        // that means here we capture every kind of event: start, update and finish
        // this makes the test to select the captured arguments based on condition
        eventCaptor.getAllValues().forEach(event -> {
            if (event instanceof WanSyncProgressUpdateEvent) {
                WanSyncProgressUpdateEvent updateEvent = (WanSyncProgressUpdateEvent) event;
                SyncKey key = new SyncKey(instance, updateEvent.getUuid(), updateEvent.getMapName());
                Integer lastRecordsSynced = recordsSyncedMap.get(key);
                int recordsSyncedUpdate = updateEvent.getRecordsSynced();
                if (lastRecordsSynced != null) {
                    assertTrue(String.format("The number of synchronized records should be monotonically increasing. Last "
                                    + "update: %d, current update: %d", lastRecordsSynced, recordsSyncedUpdate),
                            recordsSyncedUpdate >= lastRecordsSynced);
                }
                recordsSyncedMap.put(key, recordsSyncedUpdate);
                consumer.accept(key, updateEvent);
            }
        });
    }

    private void registerMcEventListener(TestContext listener) {
        sourceCluster.forEachMember(member -> {
            ManagementCenterEventListener eventListener = listener.mcEventListenerMap.get(member);
            getNode(member).getManagementCenterService().setEventListener(eventListener);
        });
    }

    private Map<String, WanSyncStats> getLastSyncResult(HazelcastInstance instance) {
        return wanReplicationPublisher(instance, wanReplication)
                .getStats()
                .getLastSyncStats();
    }

    private class TestContext {
        private final CountDownLatch syncSuspendedLatch = new CountDownLatch(sourceCluster.size());
        private final CountDownLatch syncResumeLatch = new CountDownLatch(1);
        private final AtomicInteger sentCount = new AtomicInteger();
        private final Map<HazelcastInstance, ManagementCenterEventListener> mcEventListenerMap = new HashMap<>(
                sourceCluster.size());
        private final String[] mapNames;
        private final int totalPartitions = getNode(sourceCluster.getAMember()).getPartitionService().getPartitionCount();
        private final int nonEmptyPartitions;
        private Map<SyncKey, WanSyncStats> syncStatsMap = new HashMap<>();

        private TestContext(String[] mapNames, int nonEmptyPartitions) {
            this.mapNames = mapNames;
            this.nonEmptyPartitions = nonEmptyPartitions;
        }
    }

    public static class WanBatchSyncingPublisher extends WanBatchPublisher {

        private void setup(int suspendSyncAfterRecords, TestContext testContext) {
            ((SyncingWanBatchSender) wanBatchSender).setup(testContext, suspendSyncAfterRecords);
        }
    }

    public static class SyncingWanBatchSender extends DefaultWanBatchSender {
        private final AtomicInteger eventCount = new AtomicInteger();
        private volatile int suspendSyncAfterRecords = Integer.MAX_VALUE;
        private volatile TestContext testContext;

        @Override
        public InternalCompletableFuture<Boolean> send(WanEventBatch batchReplicationEvent, Address target) {
            int syncEntryCount = batchReplicationEvent.getTotalEntryCount();

            if (eventCount.addAndGet(syncEntryCount) > suspendSyncAfterRecords) {
                try {
                    testContext.syncSuspendedLatch.countDown();
                    testContext.syncResumeLatch.await();
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }

            testContext.sentCount.addAndGet(syncEntryCount);
            return super.send(batchReplicationEvent, target);
        }

        public void setup(TestContext testContext, int suspendSyncAfterRecords) {
            this.testContext = testContext;
            this.suspendSyncAfterRecords = suspendSyncAfterRecords;
            this.eventCount.set(0);
        }
    }

    private static class SyncKey {
        private final HazelcastInstance instance;
        private final UUID uuid;
        private final String mapName;

        private SyncKey(HazelcastInstance instance, UUID uuid, String mapName) {
            this.instance = instance;
            this.uuid = uuid;
            this.mapName = mapName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SyncKey that = (SyncKey) o;

            if (!Objects.equals(instance, that.instance)) {
                return false;
            }
            if (!Objects.equals(uuid, that.uuid)) {
                return false;
            }
            return Objects.equals(mapName, that.mapName);
        }

        @Override
        public int hashCode() {
            int result = instance != null ? instance.hashCode() : 0;
            result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
            result = 31 * result + (mapName != null ? mapName.hashCode() : 0);
            return result;
        }
    }
}
