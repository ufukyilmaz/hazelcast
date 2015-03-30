package com.hazelcast.session.license;


import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Tomcat6Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class Tomcat6InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat6Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @Before
    public void setExceptionToBeThrown() throws Exception {
        exceptionToBeThrown = new InvalidLicenseException("License Key not configured!");
    }
}
