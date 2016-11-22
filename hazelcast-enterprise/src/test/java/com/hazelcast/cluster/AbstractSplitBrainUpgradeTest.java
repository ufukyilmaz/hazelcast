/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.test.SplitBrainTestSupport;
import org.junit.After;

/**
 * Abstract class for split-brain cluster upgrade tests
 */
public abstract class AbstractSplitBrainUpgradeTest extends SplitBrainTestSupport {

    protected String groupName;

    @Override
    protected void onBeforeSetup() {
        System.setProperty("hazelcast.max.join.seconds", "10");
        groupName = randomName();
    }

    @Override
    protected boolean shouldAssertAllNodesRejoined() {
        // sleep for a while to allow split brain handler to execute
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @After
    public void teardown() {
        System.clearProperty("hazelcast.max.join.seconds");
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.getGroupConfig().setName(groupName);
        return config;
    }
}
