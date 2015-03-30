package com.hazelcast.session.license;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.session.Tomcat7Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.apache.catalina.LifecycleException;
import org.junit.After;
import org.junit.Before;

public class Tomcat7InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat7Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @Before
    public void setExceptionToBeThrown() throws Exception {
        exceptionToBeThrown = new LifecycleException();
    }
}

