package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryMode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MemkindUtilTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void tearDown() {
        System.clearProperty(MemkindUtil.HD_MEMKIND);
        System.clearProperty(MemkindUtil.HD_MEMKIND_HUGEPAGES);
    }

    @Test
    public void testMountedConfiguredAndEnabled() {
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(true);

        expectedException.expect(IllegalStateException.class);
        MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
    }

    @Test
    public void testMountedConfiguredAndDisabled() {
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(false);

        expectedException.expect(IllegalStateException.class);
        MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
    }

    @Test
    public void testMountedConfiguredAndEnabledAndHdMemkindEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(true);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM, chosenKind);
    }

    @Test
    public void testMountedConfiguredAndDisabledAndHdMemkindEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(false);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM, chosenKind);
    }

    @Test
    public void testMountedConfiguredAndEnabledAndHdMemkindHugePagesEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        System.setProperty(MemkindUtil.HD_MEMKIND_HUGEPAGES, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(true);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM_HUGEPAGES, chosenKind);
    }

    @Test
    public void testMountedConfiguredAndDisabledAndHdMemkindHugePagesEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        System.setProperty(MemkindUtil.HD_MEMKIND_HUGEPAGES, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.MOUNTED)
            .setEnabled(false);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM_HUGEPAGES, chosenKind);
    }

    @Test
    public void testSystemRamConfiguredAndEnabled() {
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(true);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.PMEM_DAX_KMEM, chosenKind);
    }

    @Test
    public void testSystemRamConfiguredAndDisabled() {
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(false);

        expectedException.expect(IllegalStateException.class);
        MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
    }

    @Test
    public void testSystemRamConfiguredAndEnabledAndHdMemkindEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(true);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM, chosenKind);
    }

    @Test
    public void testSystemRamConfiguredAndEnabledAndHdMemkindDisabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(false);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM, chosenKind);
    }

    @Test
    public void testSystemRamConfiguredAndEnabledAndHdMemkindHugePagesEnabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        System.setProperty(MemkindUtil.HD_MEMKIND_HUGEPAGES, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(true);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM_HUGEPAGES, chosenKind);
    }

    @Test
    public void testSystemRamConfiguredAndDisabledAndHdMemkindHugePagesDisabled() {
        System.setProperty(MemkindUtil.HD_MEMKIND, "true");
        System.setProperty(MemkindUtil.HD_MEMKIND_HUGEPAGES, "true");
        PersistentMemoryConfig pmemConfig = new PersistentMemoryConfig()
            .setMode(PersistentMemoryMode.SYSTEM_MEMORY)
            .setEnabled(false);

        MemkindKind chosenKind = MemkindUtil.configuredKindForMemkindMalloc(pmemConfig);
        assertEquals(MemkindKind.DRAM_HUGEPAGES, chosenKind);
    }
}
