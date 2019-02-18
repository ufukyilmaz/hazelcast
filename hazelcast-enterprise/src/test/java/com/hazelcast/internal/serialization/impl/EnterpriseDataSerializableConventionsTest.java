package com.hazelcast.internal.serialization.impl;

import com.hazelcast.cache.wan.WanCacheEntryView;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
@Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/2743")
public class EnterpriseDataSerializableConventionsTest extends DataSerializableConventionsTest {

    @Override
    protected Set<Class> getWhitelistedClasses() {
        Set<Class> classes = super.getWhitelistedClasses();
        classes.add(WanCacheEntryView.class);
        return classes;
    }
}
