package com.hazelcast.session.license;

import com.hazelcast.session.Tomcat6Configurator;
import com.hazelcast.session.WebContainerConfigurator;


public class Tomcat6SecurityOnlyLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat6Configurator("hazelcast-with-security-license.xml","hazelcast-client-with-security-license.xml");
    }
}
