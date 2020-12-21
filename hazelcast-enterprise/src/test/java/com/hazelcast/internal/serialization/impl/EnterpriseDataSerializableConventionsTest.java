package com.hazelcast.internal.serialization.impl;

import com.hazelcast.cache.impl.wan.WanCacheEntryView;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class EnterpriseDataSerializableConventionsTest extends DataSerializableConventionsTest {

    private static final String[] WHITELISTED_PACKAGE_NAMES = new String[] {
            "com.hazelcast.com.fasterxml",
            "com.hazelcast.com.google.common",
            "com.hazelcast.org.apache.calcite",
            "com.hazelcast.org.codehaus",
            "com.hazelcast.org.slf4j",
            "com.hazelcast.org.snakeyaml"
    };

    @Override
    protected Set<String> getWhitelistedPackageNames() {
        return new HashSet<>(Arrays.asList(WHITELISTED_PACKAGE_NAMES));
    }

    @Override
    protected Set<Class> getWhitelistedClasses() {
        Set<Class> classes = super.getWhitelistedClasses();
        classes.add(WanCacheEntryView.class);
        classes.add(HazelcastIntegerType.class);
        classes.add(HazelcastType.class);
        return classes;
    }
}
