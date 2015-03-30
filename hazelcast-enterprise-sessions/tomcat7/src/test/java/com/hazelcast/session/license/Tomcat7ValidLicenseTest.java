package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.Tomcat7Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.After;

public class Tomcat7ValidLicenseTest extends AbstractValidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat7Configurator("hazelcast-with-valid-license.xml", "hazelcast-client-with-valid-license.xml");
    }
}


