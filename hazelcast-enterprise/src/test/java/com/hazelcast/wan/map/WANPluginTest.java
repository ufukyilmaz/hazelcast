/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public void putAll() {
        final String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        final WANPlugin wanPlugin = new WANPlugin(getNodeEngineImpl(clusterA[0]));
        wanPlugin.onStart();

        IMap<Integer, Integer> map = getMap(clusterA, MAP_NAME);
        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
        }

        assertKeysIn(clusterB, MAP_NAME, 0, 10);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                wanPlugin.run(logWriter);

                assertContains(getContent(), "WAN");
                assertContains(getContent(), "activeWanConfigName");
                assertContains(getContent(), "activePublisherName");
                assertContains(getContent(), "totalPublishLatency");
                assertContains(getContent(), "totalPublishedEventCount");
                assertContains(getContent(), "totalPublishLatency");
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
