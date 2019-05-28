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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

