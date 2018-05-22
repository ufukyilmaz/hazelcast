package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.internal.diagnostics.DiagnosticsLogWriterImpl;
import com.hazelcast.internal.diagnostics.WANPlugin;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.CharArrayWriter;
import java.io.PrintWriter;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class WANPluginTest extends MapWanReplicationTestSupport {

    private static final int EVENT_COUNTER = 1000;
    private static final String MAP_NAME = "myMap";

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
                .setProperty(WANPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");

        config.getMapConfig("default")
                .setInMemoryFormat(getMemoryFormat());
        return config;
    }

    @Test
    public void testWanDiagnosticsForActiveAndPassiveCluster() {
        final String setupName = "atob";
        setupReplicateFrom(configA, configB, singleNodeB.length, setupName, PassThroughMergePolicy.class.getName());
        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);

        final WANPlugin wanPluginClusterA = new WANPlugin(getNodeEngineImpl(singleNodeA[0]));
        final WANPlugin wanPluginClusterB = new WANPlugin(getNodeEngineImpl(singleNodeB[0]));
        wanPluginClusterA.onStart();
        wanPluginClusterB.onStart();

        IMap<Integer, Integer> map = getMap(singleNodeA, MAP_NAME);
        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
        }

        assertKeysInEventually(singleNodeB, MAP_NAME, 0, EVENT_COUNTER);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                reset();
                wanPluginClusterA.run(logWriter);
                final String content = getContent();
                assertContains(content, "WAN");
                assertContains(content, "activeWanConfigName");
                assertContains(content, "activePublisherName");
                assertContains(content, "totalPublishLatency");
                assertContains(content, "totalPublishedEventCount");
                assertContains(content, "totalPublishLatency");
                assertContains(content, "mapSentEvents-myMap");
                assertContains(content, "updateCount");
                assertContains(content, "removeCount");
                assertContains(content, "syncCount");
                assertContains(content, "droppedCount");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                reset();
                wanPluginClusterB.run(logWriter);
                final String content = getContent();
                assertContains(content, "WAN");
                assertContains(content, "mapReceivedEvents-myMap");
                assertContains(content, "updateCount");
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
