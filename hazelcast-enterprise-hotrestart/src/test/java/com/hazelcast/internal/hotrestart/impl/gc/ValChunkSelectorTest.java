package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.hotrestart.impl.di.DiContainer;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static com.hazelcast.internal.hotrestart.impl.gc.GcParams.MAX_RECORD_COUNT;
import static com.hazelcast.internal.hotrestart.impl.gc.GcParamsBuilder.gcp;
import static com.hazelcast.internal.hotrestart.impl.gc.StableChunkBuilder.chunkBuilder;
import static com.hazelcast.internal.hotrestart.impl.gc.ValChunkSelector.INITIAL_TOP_CHUNKS;
import static com.hazelcast.internal.hotrestart.impl.gc.ValChunkSelector.diagnoseChunks;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createBaseDiContainer;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ValChunkSelectorTest {

    private DiContainer di;
    private GcLogger loggerWithFinestEnabled;

    @Before
    public void setup() {
        di = createBaseDiContainer()
                .dep("gcConveyor", concurrentConveyorSingleQueue(null, new OneToOneConcurrentArrayQueue<Runnable>(1)))
                .dep("homeDir", new File(""))
                .dep(Snapshotter.class, mock(Snapshotter.class))
                .dep(MutatorCatchup.class)
                .dep(PrefixTombstoneManager.class, mock(PrefixTombstoneManager.class, withSettings().stubOnly()))
                .wireAndInitializeAll();
        loggerWithFinestEnabled = di.instantiate(GcLoggerFinestVerboseEnabled.class);
    }

    @Test
    public void when_goalsAreHigh_then_selectAllChunks() {
        final int chunkCount = INITIAL_TOP_CHUNKS + 1;
        final GcParams gcp = gcp()
                .currChunkSeq(10)
                .costGoal(Long.MAX_VALUE)
                .maxCost(Long.MAX_VALUE)
                .benefitGoal(Long.MAX_VALUE)
                .minBenefitToCost(0.02)
                .forceGc(true)
                .limitSrcChunks(true)
                .build();
        final Collection<StableChunk> allChunks = new ArrayList<StableChunk>();
        for (int i = 0; i < chunkCount; i++) {
            allChunks.add(chunkBuilder()
                    .seq(i + 1)
                    .liveRecordCount(10)
                    .size(10)
                    .garbage(9)
                    .build());
        }
        assertEquals(chunkCount, selectChunks(allChunks, gcp).size());
    }

    @Test
    public void when_tooManyRecords_thenLimitChunks() {
        final int chunkCount = INITIAL_TOP_CHUNKS + 1;
        final GcParams gcp = gcp()
                .currChunkSeq(10)
                .costGoal(Long.MAX_VALUE)
                .maxCost(Long.MAX_VALUE)
                .benefitGoal(Long.MAX_VALUE)
                .minBenefitToCost(0.02)
                .forceGc(true)
                .limitSrcChunks(true)
                .build();
        final Collection<StableChunk> allChunks = new ArrayList<StableChunk>();
        for (int i = 0; i < chunkCount; i++) {
            allChunks.add(chunkBuilder()
                    .seq(i + 1)
                    .liveRecordCount(1 + MAX_RECORD_COUNT / 10)
                    .size(10)
                    .garbage(9)
                    .build());
        }
        assertEquals(10, selectChunks(allChunks, gcp).size());
    }

    @Test
    public void when_tooManyEmptyChunks_thenLimitChunks() {
        final int chunkCount = INITIAL_TOP_CHUNKS + 1;
        final GcParams gcp = gcp()
                .currChunkSeq(10)
                .costGoal(Long.MAX_VALUE)
                .maxCost(Long.MAX_VALUE)
                .benefitGoal(Long.MAX_VALUE)
                .minBenefitToCost(0.02)
                .forceGc(true)
                .limitSrcChunks(true)
                .build();
        final Collection<StableChunk> allChunks = new ArrayList<StableChunk>();
        for (int i = 0; i < chunkCount; i++) {
            allChunks.add(chunkBuilder()
                    .seq(i + 1)
                    .liveRecordCount(10)
                    .size(10)
                    .garbage(10)
                    .build());
        }
        assertEquals(INITIAL_TOP_CHUNKS, selectChunks(allChunks, gcp).size());
    }

    @Test
    public void when_logFinestEnabled_thenDontFailInDiagnoseChunks() {
        final Collection<StableChunk> allChunks = new ArrayList<StableChunk>();
        final int chunkCount = 20;
        for (int i = 0; i < chunkCount; i++) {
            allChunks.add(chunkBuilder()
                    .seq(i + 1)
                    .liveRecordCount(10)
                    .size(8 * 1000 * 1000)
                    .garbage((chunkCount - i) * 100 * 1000)
                    .build());
        }
        diagnoseChunks(allChunks, allChunks, gcp().currChunkSeq(chunkCount).build(), loggerWithFinestEnabled);
    }

    private Collection<StableValChunk> selectChunks(Collection<StableChunk> allChunks, GcParams gcp) {
        return di.wire(new ValChunkSelector(allChunks, gcp)).select();
    }

    private static class GcLoggerFinestVerboseEnabled extends GcLogger {

        @Inject
        private GcLoggerFinestVerboseEnabled(ILogger logger) {
            super(logger);
        }

        @Override
        boolean isFinestVerboseEnabled() {
            return true;
        }
    }
}
