package com.hazelcast.session.license;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Tomcat8Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.apache.catalina.LifecycleException;
import org.junit.After;
import org.junit.Before;

public class Tomcat8InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat8Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @Before
    public void setExceptionToBeThrown() throws Exception {
        exceptionToBeThrown = new LifecycleException();
    }
}

