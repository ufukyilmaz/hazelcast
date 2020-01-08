package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.AbstractWanCustomPublisherMapTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanCustomPublisherMapTest extends AbstractWanCustomPublisherMapTest {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
                {InMemoryFormat.NATIVE}
        });
    }

    @Override
    protected Config getConfig() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addCustomPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(inMemoryFormat)
                .setWanReplicationRef(wanRef);

        Config config = inMemoryFormat == InMemoryFormat.NATIVE ? getSmallInstanceHDConfig() : smallInstanceConfig();
        return config
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }
}
