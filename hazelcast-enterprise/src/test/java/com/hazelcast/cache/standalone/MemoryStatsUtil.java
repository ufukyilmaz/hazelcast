package com.hazelcast.cache.standalone;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryStatsSupport;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author mdogan 18/02/14
 */
public class MemoryStatsUtil {

    public static LocalMemoryStats getMemoryStats(HazelcastInstance hz) {
        EnterpriseSerializationService ss = null;
        try {
            if (hz instanceof HazelcastInstanceProxy) {
                Method original = HazelcastInstanceProxy.class.getDeclaredMethod("getOriginal");
                original.setAccessible(true);
                ss = (EnterpriseSerializationService) ((HazelcastInstanceImpl) original.invoke(hz)).getSerializationService();
            } else if (hz instanceof HazelcastInstanceImpl) {
                ss = (EnterpriseSerializationService) ((HazelcastInstanceImpl) hz).getSerializationService();
            } else {
                try {
                    Class clientClass = Class.forName("com.hazelcast.client.HazelcastClientProxy");
                    if (clientClass.equals(hz.getClass())) {
                        Field f = clientClass.getDeclaredField("client");
                        f.setAccessible(true);
                        Object client = f.get(hz);
                        Method m = client.getClass().getDeclaredMethod("getSerializationService");
                        ss = (EnterpriseSerializationService) m.invoke(client);
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            return ExceptionUtil.sneakyThrow(e);
        }

        if (ss == null) {
            throw new IllegalArgumentException();
        }

        MemoryManager memoryManager = ss.getMemoryManager();
        if (memoryManager != null) {
            return memoryManager.getMemoryStats();
        }
        return MemoryStatsSupport.getMemoryStats();
    }
}
