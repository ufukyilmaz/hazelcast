package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeThatLinuxOS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MemkindMallocTest extends AbstractMallocTest {
    private final MemorySize nativeMemorySize = new MemorySize(64, MemoryUnit.MEGABYTES);

    private LibMalloc libMalloc;

    @BeforeClass
    public static void init() {
        assumeThatLinuxOS();
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(MemkindUtil.HD_MEMKIND);
    }

    @Before
    public void setup() {
        NativeMemoryConfig config = new NativeMemoryConfig();
        LibMallocFactory libMallocFactory = new MemkindMallocFactory(config);
        libMalloc = libMallocFactory.create(nativeMemorySize.bytes());
    }

    @Override
    LibMalloc getLibMalloc() {
        return libMalloc;
    }

}
