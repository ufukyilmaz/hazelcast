package com.hazelcast.session.license;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.Tomcat8Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.After;

public class Tomcat8InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat8Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }
}

