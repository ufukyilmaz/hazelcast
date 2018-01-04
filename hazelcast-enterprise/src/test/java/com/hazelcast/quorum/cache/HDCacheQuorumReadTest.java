package com.hazelcast.quorum.cache;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.quorum.map.MapQuorumReadTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sun.misc.Cache;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HDCacheQuorumReadTest extends CacheQuorumReadTest {

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(getHDConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

}
