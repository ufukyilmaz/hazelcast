package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.Java6ExcludeRule;
import com.hazelcast.session.JettyConfigurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.After;
import org.junit.Rule;


public class Jetty9SecurityOnlyLicenseTest extends AbstractInvalidLicenseTest {

    @Rule
    public Java6ExcludeRule java6ExcludeRule = new Java6ExcludeRule();

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new JettyConfigurator("hazelcast-with-security-license.xml","hazelcast-client-with-security-license.xml");
    }

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        instance1.stop();
    }

}
