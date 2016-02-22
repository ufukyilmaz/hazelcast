package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.INITIAL_TOP_CHUNKS;
import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.diagnoseChunks;
import static com.hazelcast.spi.hotrestart.impl.gc.ChunkSelector.selectChunksToCollect;
import static com.hazelcast.spi.hotrestart.impl.gc.GcParams.MAX_RECORD_COUNT;
import static com.hazelcast.spi.hotrestart.impl.gc.GcParamsBuilder.gcp;
import static com.hazelcast.spi.hotrestart.impl.gc.StableChunkBuilder.chunkBuilder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkSelectorTest {

    private PrefixTombstoneManager pfixTombstoMgr;
    private GcExecutor.MutatorCatchup mc;
    private GcLogger logger;
    private GcLogger loggerWithFinestEnabled;

    @Before public void setup() {
        this.pfixTombstoMgr = mock(PrefixTombstoneManager.class, withSettings().stubOnly());
        this.mc = mock(GcExecutor.MutatorCatchup.class, withSettings().stubOnly());
        final ILogger ilogger = createLoggingService().getLogger("com.hazelcast.spi.hotrestart");
        this.logger = new GcLogger(ilogger);
        this.loggerWithFinestEnabled = new GcLoggerFinestEnabled(ilogger);
    }

    @Test public void when_goalsAreHigh_then_selectAllChunks() {
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
        assertEquals(chunkCount, selectChunks(allChunks, gcp).srcChunks.size());
    }

    @Test public void when_tooManyRecords_thenLimitChunks() {
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
        assertEquals(10, selectChunks(allChunks, gcp).srcChunks.size());
    }

    @Test public void when_tooManyEmptyChunks_thenLimitChunks() {
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
        assertEquals(INITIAL_TOP_CHUNKS, selectChunks(allChunks, gcp).srcChunks.size());
    }

    @Test public void when_logFinestEnabled_thenDontFailInDiagnoseChunks() {
        final Collection<StableChunk> allChunks = new ArrayList<StableChunk>();
        final int chunkCount = 20;
        for (int i = 0; i < chunkCount; i++) {
            allChunks.add(chunkBuilder()
                    .seq(i + 1)
                    .liveRecordCount(10)
                    .size(8*1000*1000)
                    .garbage((chunkCount - i) * 100 * 1000)
                    .build());
        }
        diagnoseChunks(allChunks, allChunks, gcp().currChunkSeq(chunkCount).build(), loggerWithFinestEnabled);
    }

    private ChunkSelector.ChunkSelection selectChunks(Collection<StableChunk> allChunks, GcParams gcp) {
        return selectChunksToCollect(allChunks, gcp, pfixTombstoMgr, mc, logger);
    }

    private static class GcLoggerFinestEnabled extends GcLogger {
        GcLoggerFinestEnabled(ILogger logger) {
            super(logger);
        }

        @Override boolean isFinestEnabled() {
            return true;
        }
    }
}
