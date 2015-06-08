package com.hazelcast.session.license;

import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Java6ExcludeRule;
import com.hazelcast.session.Tomcat8Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.apache.catalina.LifecycleException;
import org.hamcrest.Matcher;
import org.junit.Rule;

public class Tomcat8InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Rule
    public Java6ExcludeRule java6ExcludeRule = new Java6ExcludeRule();

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat8Configurator("hazelcast-without-license.xml", "hazelcast-client-without-license.xml");
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
