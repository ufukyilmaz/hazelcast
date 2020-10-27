package com.hazelcast.sql_slow;

import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.sql.index.HDSqlIndexTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class HDSqlIndexSlowTest extends HDSqlIndexTest {
    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}, field1:{2}, field2:{3}")
    public static Collection<Object[]> parameters() {
        return parametersSlow();
    }
}
