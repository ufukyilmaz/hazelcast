package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartTestSupport;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractMapHotRestartTest extends HotRestartTestSupport {

    protected static final int KEY_COUNT = 1000;

    @Parameter
    public InMemoryFormat memoryFormat;

    @Parameter(1)
    public int keyRange;

    @Parameter(2)
    public boolean fsyncEnabled;

    @Parameter(3)
    public boolean evictionEnabled;

    String mapName;

    protected void setupMapInternal() {

    }

    @Override
    protected final void setupInternal() {
        mapName = randomMapName();
        setupMapInternal();
    }

    HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(1);
    }

    HazelcastInstance newHazelcastInstance(final int backupCount) {
        return newHazelcastInstance(new IFunction<Address, Config>() {
            @Override
            public Config apply(Address input) {
                return makeConfig(input, backupCount);
            }
        });
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        return newInstances(clusterSize, 1);
    }

    HazelcastInstance[] newInstances(int clusterSize, int backupCount) {
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance instance = newHazelcastInstance(backupCount);
            instances[i] = instance;
        }
        return instances;
    }

    HazelcastInstance[] restartInstances(int clusterSize) {
        return restartInstances(clusterSize, 1);
    }

    HazelcastInstance[] restartInstances(int clusterSize, final int backupCount) {
        return restartCluster(clusterSize, new IFunction<Address, Config>() {
            @Override
            public Config apply(Address address) {
                return makeConfig(address, backupCount);
            }
        });
    }

    Config makeConfig(Address address, int backupCount) {
        Config config = new Config()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                // to reduce used native memory size
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())));

        if (memoryFormat == InMemoryFormat.NATIVE) {
            config.getNativeMemoryConfig()
                    .setEnabled(true)
                    .setSize(getNativeMemorySize())
                    .setMetadataSpacePercentage(20);
        }

        if (memoryFormat != null) {
            MapConfig mapConfig = new MapConfig(mapName)
                    .setInMemoryFormat(memoryFormat)
                    .setBackupCount(backupCount);
            mapConfig.getHotRestartConfig()
                    .setEnabled(true)
                    .setFsync(fsyncEnabled);
            setEvictionConfig(mapConfig);
            config.addMapConfig(mapConfig);
        }

        return config;
    }

    MemorySize getNativeMemorySize() {
        return new MemorySize(64, MemoryUnit.MEGABYTES);
    }

    <V> IMap<Integer, V> createMap() {
        HazelcastInstance hz = getFirstInstance();
        assertNotNull(hz);
        return createMap(hz);
    }

    <V> IMap<Integer, V> createMap(HazelcastInstance hz) {
        return hz.getMap(mapName);
    }

    private void setEvictionConfig(MapConfig mapConfig) {
        if (!evictionEnabled) {
            return;
        }
        mapConfig.setEvictionPolicy(EvictionPolicy.LFU);
        if (memoryFormat == InMemoryFormat.NATIVE) {
            mapConfig.setMaxSizeConfig(new MaxSizeConfig().setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE).setSize(80));
        } else {
            mapConfig.setMaxSizeConfig(new MaxSizeConfig().setMaxSizePolicy(PER_PARTITION).setSize(50));
        }
    }
}
