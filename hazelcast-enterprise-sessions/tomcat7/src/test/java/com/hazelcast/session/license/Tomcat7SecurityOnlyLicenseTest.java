package com.hazelcast.session.license;



import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Tomcat7Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.apache.catalina.LifecycleException;
import org.hamcrest.Matcher;


public class Tomcat7SecurityOnlyLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat7Configurator("hazelcast-with-security-license.xml","hazelcast-client-with-security-license.xml");
    }

    @Override
    protected Matcher<? extends Throwable> getCause() {
        return org.hamcrest.CoreMatchers.instanceOf(InvalidLicenseException.class);
    }

    @Override
    protected Class<? extends Throwable> getException() {
        return LifecycleException.class;
    }

}

