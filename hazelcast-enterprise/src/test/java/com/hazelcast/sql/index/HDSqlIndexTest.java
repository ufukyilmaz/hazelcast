package com.hazelcast.sql.index;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDSqlIndexTest extends SqlIndexAbstractTest {
    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDIndexConfig();
    }

    @Override
    protected MapConfig getMapConfig() {
        return super.getMapConfig().setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Override
    protected boolean isHd() {
        return true;
    }

    @Override
    protected int getMemberCount() {
        return 1;
    }
}
