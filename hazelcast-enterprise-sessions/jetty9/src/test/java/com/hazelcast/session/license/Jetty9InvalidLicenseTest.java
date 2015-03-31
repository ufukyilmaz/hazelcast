package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Java6ExcludeRule;
import com.hazelcast.session.JettyConfigurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Rule;


public class Jetty9InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Rule
    public Java6ExcludeRule java6ExcludeRule = new Java6ExcludeRule();

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new JettyConfigurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        instance1.stop();
    }

}
