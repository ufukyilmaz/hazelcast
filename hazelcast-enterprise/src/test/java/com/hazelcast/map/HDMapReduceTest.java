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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.map.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapReduceTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMapReduce_throws_IllegalArgumentException_whenInMemoryFormat_NATIVE() throws Exception {
        Config config = getHDConfig();
        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("NATIVE storage format is not supported for MapReduce");

        map.aggregate(new NOOPSupplier(), new NOOPAggregation());
    }


    private final class NOOPSupplier extends Supplier {
        @Override
        public Object apply(Map.Entry entry) {
            return null;
        }
    }

    private final class NOOPAggregation implements Aggregation {
        @Override
        public Collator getCollator() {
            return null;
        }

        @Override
        public Mapper getMapper(Supplier supplier) {
            return new Mapper() {
                @Override
                public void map(Object key, Object value, Context context) {

                }
            };
        }

        @Override
        public CombinerFactory getCombinerFactory() {
            return null;
        }

        @Override
        public ReducerFactory getReducerFactory() {
            return new ReducerFactory() {
                @Override
                public Reducer newReducer(Object key) {
                    return null;
                }
            };
        }
    }
}

