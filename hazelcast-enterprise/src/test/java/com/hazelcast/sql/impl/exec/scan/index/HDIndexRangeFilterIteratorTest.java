package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDIndexRangeFilterIteratorTest extends IndexRangeFilterIteratorTest {
    @Override
    protected Config getConfig() {
        MapConfig mapConfig = new MapConfig().setName(MAP_NAME).setInMemoryFormat(InMemoryFormat.NATIVE);

        return HDTestSupport.getSmallInstanceHDIndexConfig().addMapConfig(mapConfig);
    }
}
