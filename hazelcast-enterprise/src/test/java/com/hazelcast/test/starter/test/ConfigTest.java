package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.starter.Configuration.configForClassLoader;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigTest {

    @Test
    public void configCloneTest() throws Exception {
        configClone(getClass().getClassLoader());
    }

    public void configClone(ClassLoader cl) throws Exception {
        Config thisConfig = new Config();
        thisConfig.setInstanceName("TheAssignedName");
        thisConfig.addMapConfig(new MapConfig("myMap"));

        thisConfig.addListConfig(new ListConfig("myList"));

        thisConfig.addListenerConfig(new ListenerConfig("the.listener.config.class"));

        Config otherConfig = (Config) configForClassLoader(thisConfig, cl);
        assertEquals(otherConfig.getInstanceName(), thisConfig.getInstanceName());
        assertEquals(otherConfig.getMapConfigs().size(), thisConfig.getMapConfigs().size());
        assertEquals(otherConfig.getListConfigs().size(), thisConfig.getListConfigs().size());
        assertEquals(otherConfig.getListenerConfigs().size(), thisConfig.getListenerConfigs().size());
    }
}
