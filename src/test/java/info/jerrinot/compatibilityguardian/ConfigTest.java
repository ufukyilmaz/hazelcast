package info.jerrinot.compatibilityguardian;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import org.junit.Test;

import static info.jerrinot.compatibilityguardian.Configuration.*;
import static org.junit.Assert.*;

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

        Config otherConfig = (Config) configLoader(thisConfig, cl);
        assertEquals(otherConfig.getInstanceName(), thisConfig.getInstanceName());
        assertEquals(otherConfig.getMapConfigs().size(), thisConfig.getMapConfigs().size());
        assertEquals(otherConfig.getListConfigs().size(), thisConfig.getListConfigs().size());
        assertEquals(otherConfig.getListenerConfigs().size(), thisConfig.getListenerConfigs().size());
    }
}
