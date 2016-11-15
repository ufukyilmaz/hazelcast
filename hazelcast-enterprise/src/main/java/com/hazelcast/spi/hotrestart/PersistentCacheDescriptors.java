package com.hazelcast.spi.hotrestart;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.util.ExceptionUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.nio.Bits.combineToLong;
import static com.hazelcast.nio.Bits.extractInt;
import static com.hazelcast.nio.IOUtil.toFileName;
import static java.lang.Math.max;

/**
 * Maintains the in-memory mapping of names and 32-bit cache IDs to {@link CacheDescriptor}s. The names are generated from
 * the service name and distributed object name and the cache ID is generated by a sequence. Also persists this
 * information on disk with an additional config per cache descriptor.
 */
public class PersistentCacheDescriptors {

    private static final String CONFIG_SUFFIX = ".config";
    private static final String CONFIG_FOLDER = "configs";

    /**
     * Mappings from a combination of service name and distributed object name to cache descriptor
     */
    private final Map<String, CacheDescriptor> nameToDesc = new ConcurrentHashMap<String, CacheDescriptor>();
    /**
     * Mappings from a combination of cache ID to cache descriptor
     */
    private final Map<Integer, CacheDescriptor> idToDesc = new ConcurrentHashMap<Integer, CacheDescriptor>();
    private final File configsDir;
    private volatile int cacheIdSeq;

    public PersistentCacheDescriptors(File instanceHome) {
        configsDir = new File(instanceHome, CONFIG_FOLDER);
        ensureConfigDirectoryExists();
    }

    /**
     * Checks if the configuration directory exists and creates one if not.
     */
    public void ensureConfigDirectoryExists() {
        if (!configsDir.exists() && !configsDir.mkdirs()) {
            throw new HotRestartException("Cannot create config directory: " + configsDir.getAbsolutePath());
        }
    }

    @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    public void reset() {
        synchronized (nameToDesc) {
            nameToDesc.clear();
            idToDesc.clear();
            cacheIdSeq = 0;
        }
        ensureConfigDirectoryExists();
    }


    /**
     * Returns the prefix for the given parameters. The prefix is generated from the cache ID and the given {@code partitionId}.
     * The cache ID is retrieved from the persisted cache descriptors map by {@code serviceName} and {@code name}.
     *
     * @param serviceName the service name
     * @param name        the distributed object name
     * @param partitionId the partition ID
     * @return the prefix
     */
    public long getPrefix(String serviceName, String name, int partitionId) {
        final String key = toCacheKey(serviceName, name);
        final CacheDescriptor desc = nameToDesc.get(key);
        if (desc == null) {
            throw new IllegalArgumentException("Unknown name! " + key);
        }
        return combineToLong(desc.getId(), partitionId);
    }

    /**
     * Returns the {@link CacheDescriptor} for the given {@code prefix}. The higher 4 bytes in the {@code prefix} should
     * represent the cache ID.
     *
     * @param prefix the prefix for which the {@link CacheDescriptor} is returned
     * @return the cache descriptor
     */
    public CacheDescriptor getDescriptor(long prefix) {
        return idToDesc.get(extractInt(prefix, false));
    }

    /**
     * Ensures that this instance contains mapping for a {@link CacheDescriptor}. If there is no existing
     * mapping, creates one by assigning it a new {@code ID}. It then persists the newly created descriptor on disk.
     *
     * @param serializationService the serialization service used for serializing a newly created descriptor to disk
     * @param serviceName          the name of the service (e.g. {@link com.hazelcast.cache.impl.CacheService#SERVICE_NAME} or
     *                             {@link com.hazelcast.map.impl.MapService#SERVICE_NAME})
     * @param name                 the name of the distributed object
     * @param config               configuration written to disk alongside the newly created descriptor
     */
    @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    public void ensureHas(InternalSerializationService serializationService, String serviceName, String name, Object config) {
        String cacheKey = toCacheKey(serviceName, name);
        if (nameToDesc.get(cacheKey) != null) {
            return;
        }
        synchronized (nameToDesc) {
            if (nameToDesc.get(cacheKey) != null) {
                return;
            }
            final int id = ++cacheIdSeq;
            final CacheDescriptor desc = new CacheDescriptor(serviceName, name, id);
            ObjectDataOutputStream out = null;
            try {
                String configFileName = configFileName(serviceName, name);
                File configFile = new File(configsDir, configFileName);
                if (configFile.exists()) {
                    throw new IllegalArgumentException(configFile + " already exists!");
                }
                out = createObjectDataOutputStream(new FileOutputStream(configFile), serializationService);
                out.writeUTF(serviceName);
                out.writeInt(id);
                out.writeUTF(name);
                out.writeObject(config);
                idToDesc.put(id, desc);
                nameToDesc.put(cacheKey, desc);
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            } finally {
                IOUtil.closeResource(out);
            }
        }
    }

    private static String configFileName(String serviceName, String name) {
        return toFileName(serviceName) + '-' + toFileName(name) + CONFIG_SUFFIX;
    }

    /**
     * Reloads the cache descriptor data from disk. The persisted data should be kept in the {@link #CONFIG_FOLDER} under the
     * {@code instanceHome} defined by the constructor parameter and the files should have the suffix {@link #CONFIG_SUFFIX}.
     * Aborts loading if the cache ID sequence is not 0 (the instance was already restored or used to create a new persistent
     * cache).
     * <p>
     * Notifies the {@code loadedConfigurationListeners} with the loaded descriptors and configurations
     *
     * @param serializationService         the serialization service for deserializing the persisted descriptors
     * @param loadedConfigurationListeners listeners to be notified with the loaded descriptors and configurations
     */
    @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    void restore(InternalSerializationService serializationService,
                 List<LoadedConfigurationListener> loadedConfigurationListeners) {
        if (cacheIdSeq != 0) {
            return;
        }
        synchronized (nameToDesc) {
            if (cacheIdSeq != 0) {
                return;
            }
            File[] configFiles = configsDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File file, String name) {
                    return name.endsWith(CONFIG_SUFFIX);
                }
            });
            if (configFiles == null) {
                return;
            }
            int maxId = 0;
            for (File configFile : configFiles) {
                CacheDescriptor desc;
                Object config;
                ObjectDataInputStream in = null;
                try {
                    in = createObjectDataInputStream(new FileInputStream(configFile), serializationService);
                    String serviceName = in.readUTF();
                    int id = in.readInt();
                    String name = in.readUTF();
                    config = in.readObject();
                    desc = new CacheDescriptor(serviceName, name, id);
                } catch (IOException e) {
                    throw ExceptionUtil.rethrow(e);
                } finally {
                    IOUtil.closeResource(in);
                }
                maxId = max(maxId, desc.getId());
                String key = toCacheKey(desc.getServiceName(), desc.getName());
                nameToDesc.put(key, desc);
                idToDesc.put(desc.getId(), desc);
                if (config != null) {
                    notifyListeners(loadedConfigurationListeners, desc, config);
                }
            }
            cacheIdSeq = maxId;
        }
    }

    private void notifyListeners(List<LoadedConfigurationListener> loadedConfigurationListeners,
                                 CacheDescriptor desc, Object config) {
        for (LoadedConfigurationListener listener : loadedConfigurationListeners) {
            listener.onConfigurationLoaded(desc.getServiceName(), desc.getName(), config);
        }
    }

    private static String toCacheKey(String serviceName, String name) {
        return serviceName + "::" + name;
    }

    /**
     * Returns the partition ID for the {@code prefix}. Currently the lower 4 bytes of the {@code prefix} represent the
     * partition ID.
     *
     * @param prefix the prefix
     * @return the partition ID
     */
    public static int toPartitionId(long prefix) {
        return extractInt(prefix, true);
    }
}
