package com.hazelcast.session.license;

import com.hazelcast.session.Tomcat6Configurator;
import com.hazelcast.session.WebContainerConfigurator;

public class Tomcat6ValidLicenseTest extends AbstractValidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat6Configurator("hazelcast-with-valid-license.xml", "hazelcast-client-with-valid-license.xml");
    }
}
