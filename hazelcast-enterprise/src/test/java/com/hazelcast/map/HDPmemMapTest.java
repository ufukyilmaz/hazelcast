package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.memory.impl.MemkindHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.util.OsHelper.isLinux;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDPmemMapTest extends HazelcastTestSupport {

    private static final String NATIVE_MAP = "NativeMap";
    private static final int INITIAL_MAP_SIZE = 10_000;

    @BeforeClass
    public static void init() {
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

    private static void configurePmemDirectories(NativeMemoryConfig config, String pmemDirectories) {
        assert !StringUtil.isNullOrEmptyAfterTrim(pmemDirectories);

        int node = 0;
        for (String pmemDirectory : pmemDirectories.split(",")) {
            config.getPersistentMemoryConfig().addDirectoryConfig(new PersistentMemoryDirectoryConfig(pmemDirectory, node++));
        }
    }

    @Test
    public void endToEndTest() {
        assumeTrue(checkPlatform());
        Config hdConfig = getHDConfig();
        configurePmemDirectories(hdConfig.getNativeMemoryConfig(), PERSISTENT_MEMORY_DIRECTORIES);
        hdConfig.getMapConfig(NATIVE_MAP).setInMemoryFormat(NATIVE);

        HazelcastInstance hzInstance = createHazelcastInstance(hdConfig);

        // populate
        IMap<Integer, Integer> map = hzInstance.getMap(NATIVE_MAP);
        for (int i = 0; i < INITIAL_MAP_SIZE; i++) {
            map.put(i, i);
        }

        // update every fifth entry to 42
        map.executeOnEntries((EntryProcessor<Integer, Integer, Integer>) entry -> {
            if (entry.getKey() % 5 == 0) {
                entry.setValue(42);
            }
            return 42;
        });

        // count 42s
        MutableInteger count42s = new MutableInteger();
        map.forEach((key, value) -> {
            if (value == 42) {
                count42s.getAndInc();
            }
        });

        int expected42s = INITIAL_MAP_SIZE / 5 + 1;
        assertEquals(expected42s, count42s.value);

        // delete every fifth entry
        map.executeOnEntries((EntryProcessor<Integer, Integer, Integer>) entry -> {
            if (entry.getKey() % 5 == 0) {
                entry.setValue(null);
            }
            return null;
        });

        int expectedMapSize = INITIAL_MAP_SIZE - (INITIAL_MAP_SIZE / 5);
        assertEquals(expectedMapSize, map.size());
    }

    static boolean checkPlatform() {
        return isLinux() && !is32bitJVM();
    }
}
