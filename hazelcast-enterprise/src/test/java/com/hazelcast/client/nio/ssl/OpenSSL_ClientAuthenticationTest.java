package com.hazelcast.client.nio.ssl;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.TestEnvironmentUtil.assumeNoIbmJvm;
import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class OpenSSL_ClientAuthenticationTest extends ClientAuthenticationTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeThatOpenSslIsSupported();
        // ClientAuthenticationTest doesn't use the OpenSSL native configuration (keyFile, ...)
        // and the Java way is not supported on IBM Java versions
        assumeNoIbmJvm();
    }

    public OpenSSL_ClientAuthenticationTest() {
        openSsl = true;
    }
}
