package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.internal.util.StringUtil;

public class PmemTestUtil {
    public static String firstOf(String pmemDirectories) {
        assert !StringUtil.isNullOrEmptyAfterTrim(pmemDirectories);

        int commaIndex = pmemDirectories.indexOf(',');
        return pmemDirectories.substring(0, commaIndex >= 0 ? commaIndex : pmemDirectories.length());
    }

    public static void configurePmemDirectories(NativeMemoryConfig config, String pmemDirectories) {
        assert !StringUtil.isNullOrEmptyAfterTrim(pmemDirectories);

        for (String pmemDirectory : pmemDirectories.split(",")) {
            config.getPersistentMemoryConfig().addDirectoryConfig(new PersistentMemoryDirectoryConfig(pmemDirectory));
        }
    }
}
