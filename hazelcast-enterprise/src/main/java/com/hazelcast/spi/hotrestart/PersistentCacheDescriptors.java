package com.hazelcast.spi.hotrestart;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.nio.Bits.combineToLong;
import static com.hazelcast.nio.Bits.extractInt;
import static com.hazelcast.spi.hotrestart.CacheDescriptor.toNonProvisionalName;
import static java.lang.Math.max;

/**
 * Maintains the mapping between name and a 32-bit ID stored with
 * each record in the Hot Restart store.
 */
public class PersistentCacheDescriptors {
    private static final String CONFIG_SUFFIX = ".config";
    private static final String CONFIG_FOLDER = "configs";

    private final Map<String, CacheDescriptor> nameToDesc = new ConcurrentHashMap<String, CacheDescriptor>();
    private final Map<Integer, CacheDescriptor> idToDesc = new ConcurrentHashMap<Integer, CacheDescriptor>();
    private final Map<String, Object> provisionalConfigurations = new ConcurrentHashMap<String, Object>();
    private final File configsDir;
    private volatile int cacheIdSeq;

    public PersistentCacheDescriptors(File instanceHome) {
        configsDir = new File(instanceHome, CONFIG_FOLDER);
        if (!configsDir.exists()) {
            boolean mkdirs = configsDir.mkdirs();
            assert mkdirs : "Cannot create configs directory!";
        }
    }

    public long getPrefix(String name, int partitionId) {
        final CacheDescriptor desc = nameToDesc.get(toNonProvisionalName(name));
        if (desc == null) {
            throw new IllegalArgumentException("Unknown name! " + name);
        }
        return combineToLong(desc.getId(), partitionId);
    }

    public CacheDescriptor getDescriptor(long prefix) {
        return idToDesc.get(extractInt(prefix, false));
    }

    public void ensureHas(SerializationService serializationService, String serviceName, String name, Object config) {
        if (nameToDesc.get(name) != null) {
            return;
        }
        synchronized (nameToDesc) {
            if (nameToDesc.get(name) != null) {
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
                nameToDesc.put(name, desc);

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

    static String toFileName(String name) {
        return name.replaceAll("[:\\\\/*\"?|<>']", "_");
    }

    void restore(SerializationService serializationService) {
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
                nameToDesc.put(desc.getName(), desc);
                idToDesc.put(desc.getId(), desc);
                if (config != null) {
                    final String key = toProvisionalConfigKey(desc.getServiceName(), desc.getProvisionalName());
                    provisionalConfigurations.put(key, config);
                }
            }
            cacheIdSeq = maxId;
        }
    }

    private static String toProvisionalConfigKey(String serviceName, String provisionalName) {
        return serviceName + "::" + provisionalName;
    }

    void clearProvisionalConfigs() {
        provisionalConfigurations.clear();
    }

    public Object getProvisionalConfig(String serviceName, String name) {
        return provisionalConfigurations.get(toProvisionalConfigKey(serviceName, name));
    }

    public static int toPartitionId(long prefix) {
        return extractInt(prefix, true);
    }
}
