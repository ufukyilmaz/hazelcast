package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.session.Java6ExcludeRule;
import com.hazelcast.session.JettyConfigurator;
import com.hazelcast.session.WebContainerConfigurator;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class Jetty9SecurityOnlyLicenseTest extends AbstractInvalidLicenseTest {

    @Rule
    public Java6ExcludeRule java6ExcludeRule = new Java6ExcludeRule();

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new JettyConfigurator("hazelcast-with-security-license.xml", "hazelcast-client-with-security-license.xml");
    }

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        instance1.stop();
    }
}
