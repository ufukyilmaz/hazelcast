package com.hazelcast.client.nio.ssl;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.TestEnvironmentUtil.assumeThatNoIbmJvm;
import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsAvailable;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class OpenSSL_ClientAuthenticationTest extends ClientAuthenticationTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeThatOpenSslIsAvailable();
        assumeThatNoIbmJvm();
    }

    public OpenSSL_ClientAuthenticationTest() {
        openSsl = true;
    }
}
