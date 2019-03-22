package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyRestEndpointTest extends AbstractSecureOneEndpointTest {

    // When using IBM JSSE2 and getting the SSLContext with "TLS" as protocol
    // JSSE2 will only enable usage of TLS V1.0 even though other protocols are
    // supported. To bring it in line with the Oracle implementation and allow
    // TLS V1.1 and TLS V1.2, we need to set this property.
    // see:
    // https://github.com/hazelcast/hazelcast-enterprise/issues/2824
    // https://www.ibm.com/support/knowledgecenter/en/SSYKE2_7.1.0/com.ibm.java.security.component.71.doc/security-component/jsse2Docs/matchsslcontext_tls.html#matchsslcontext_tls
    @Rule
    public OverridePropertyRule rule = OverridePropertyRule.set("com.ibm.jsse2.overrideDefaultTLS", "true");

    @Test
    @Override
    public void testRestConnectionToEndpoints() throws Exception {
        testTextEndpoint(REST_PORT, restTruststore, REST_PASSWORD, true);
    }

    @Override
    protected Properties prepareRestEndpointSsl() {
        return prepareSslProperties(memberForRestKeystore, REST_PASSWORD);
    }
}
