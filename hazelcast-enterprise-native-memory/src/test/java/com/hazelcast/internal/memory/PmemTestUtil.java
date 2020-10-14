package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.internal.util.StringUtil;

import static com.hazelcast.config.PersistentMemoryMode.MOUNTED;

public class PmemTestUtil {
    public static String firstOf(String pmemDirectories) {
        assert !StringUtil.isNullOrEmptyAfterTrim(pmemDirectories);

        int commaIndex = pmemDirectories.indexOf(',');
        return pmemDirectories.substring(0, commaIndex >= 0 ? commaIndex : pmemDirectories.length());
    }

    public static void configurePmemDirectories(NativeMemoryConfig config, String pmemDirectories) {
        assert !StringUtil.isNullOrEmptyAfterTrim(pmemDirectories);

        int node = 0;
        PersistentMemoryConfig pmemConfig = config.getPersistentMemoryConfig();
        pmemConfig.setEnabled(true);
        pmemConfig.setMode(MOUNTED);
        for (String pmemDirectory : pmemDirectories.split(",")) {
            pmemConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(pmemDirectory, node++));
        }
    }
}
