package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.nearcache.impl.nativememory.NativeMemoryNearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.nativememory.SegmentedNativeMemoryNearCacheRecordStore;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMemberMapReconciliationTest extends MemberMapReconciliationTest {

    @Parameters(name = "mapInMemoryFormat:{0} nearCacheInMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, BINARY},
                {BINARY, OBJECT},
                {BINARY, NATIVE},

                {OBJECT, BINARY},
                {OBJECT, OBJECT},
                {OBJECT, NATIVE},

                {NATIVE, BINARY},
                {NATIVE, OBJECT},
                {NATIVE, NATIVE},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected StaleReadDetector getStaleReadDetector(NearCacheRecordStore nearCacheRecordStore) {
        if (nearCacheInMemoryFormat == NATIVE) {
            NativeMemoryNearCacheRecordStore[] segments
                    = ((SegmentedNativeMemoryNearCacheRecordStore) nearCacheRecordStore).getSegments();
            return segments[0].getStaleReadDetector();
        } else {
            return super.getStaleReadDetector(nearCacheRecordStore);
        }
    }
}
