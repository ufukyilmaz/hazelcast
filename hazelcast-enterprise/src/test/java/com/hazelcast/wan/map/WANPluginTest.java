package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.internal.diagnostics.DiagnosticsLogWriterImpl;
import com.hazelcast.internal.diagnostics.WANPlugin;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.CharArrayWriter;
import java.io.PrintWriter;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WANPluginTest extends MapWanReplicationTestSupport {

    private static final int EVENT_COUNTER = 1000;
    public static final String MAP_NAME_A = "mapA";
    public static final String MAP_NAME_B = "mapB";

    protected DiagnosticsLogWriterImpl logWriter;
    private CharArrayWriter out;

    @Before
    @Override
    public final void setup() {
        super.setup();
        logWriter = new DiagnosticsLogWriterImpl();
        out = new CharArrayWriter();
        logWriter.init(new PrintWriter(out));
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig()
                             .setProperty(WANPlugin.PERIOD_SECONDS.getName(), "1");

        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());
        return config;
    }

    @Test
    public void testWanDiagnosticsForActiveAndPassiveCluster() {
        final String unusedSetupName = "unusedSetup";
        final String setupName = "atob";
        setupReplicateFrom(configA, configB, singleNodeB.length, unusedSetupName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configB, singleNodeB.length, setupName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, singleNodeC.length, setupName, PassThroughMergePolicy.class.getName());

        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);
        initCluster(singleNodeC, configC);

        final WANPlugin wanPluginClusterA = new WANPlugin(getNodeEngineImpl(singleNodeA[0]));
        final WANPlugin wanPluginClusterB = new WANPlugin(getNodeEngineImpl(singleNodeB[0]));
        final WANPlugin wanPluginClusterC = new WANPlugin(getNodeEngineImpl(singleNodeC[0]));
        wanPluginClusterA.onStart();
        wanPluginClusterB.onStart();
        wanPluginClusterC.onStart();

        IMap<Integer, Integer> map = getMap(singleNodeA, MAP_NAME_A);
        IMap<Integer, Integer> map2 = getMap(singleNodeA, MAP_NAME_B);
        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
            map2.put(i, i);
        }

        assertKeysInEventually(singleNodeB, MAP_NAME_A, 0, EVENT_COUNTER);
        assertKeysInEventually(singleNodeB, MAP_NAME_B, 0, EVENT_COUNTER);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                reset();
                wanPluginClusterA.run(logWriter);
                final String content = getContent();
                assertContains(content, "WAN");
                assertContains(content, configB.getGroupConfig().getName() + "[");
                assertContains(content, configC.getGroupConfig().getName() + "[");
                assertContains(content, "activeWanConfigName");
                assertContains(content, "activePublisherName");
                assertContains(content, "totalPublishLatency");
                assertContains(content, "totalPublishedEventCount=2,000");
                assertContains(content, "totalPublishLatency");
                assertContains(content, "mapSentEvents-mapA");
                assertContains(content, "mapSentEvents-mapB");
                assertContains(content, "updateCount=1,000");
                assertContains(content, "updateCount=2,000");
                assertContains(content, "removeCount");
                assertContains(content, "syncCount");
                assertContains(content, "droppedCount");
                assertContains(content, setupName);
                assertContains(content, unusedSetupName);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                reset();
                wanPluginClusterB.run(logWriter);
                final String content = getContent();
                assertContains(content, "WAN");
                assertContains(content, "mapReceivedEvents-mapA");
                assertContains(content, "mapReceivedEvents-mapB");
                assertContains(content, "updateCount=1,000");
                assertContains(content, "updateCount=2,000");
                assertContains(content, "removeCount");
                assertContains(content, "syncCount");
            }
        });
    }


    protected void reset() {
        out.reset();
    }

    private String getContent() {
        return out.toString();
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

}
