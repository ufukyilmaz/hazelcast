/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class MapPartitionClearOperationTest extends HazelcastTestSupport {

    /**
     * This test calls internally {@link com.hazelcast.map.impl.operation.EnterpriseMapPartitionClearOperation}.
     */
    @Test
    public void testMapShutdown_finishesSuccessfully() throws Exception {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap map = node.getMap("default");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        try {
            node.shutdown();
        } catch (Exception e) {
            fail();
        }
    }

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }

}
