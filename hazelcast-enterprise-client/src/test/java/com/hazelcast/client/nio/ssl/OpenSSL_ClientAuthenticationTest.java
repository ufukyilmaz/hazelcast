package com.hazelcast.client.nio.ssl;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import io.netty.handler.ssl.OpenSsl;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assume.assumeTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class OpenSSL_ClientAuthenticationTest extends ClientAuthenticationTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeTrue(OpenSsl.isAvailable());
    }

    public OpenSSL_ClientAuthenticationTest() {
        openSsl = true;
    }
}
