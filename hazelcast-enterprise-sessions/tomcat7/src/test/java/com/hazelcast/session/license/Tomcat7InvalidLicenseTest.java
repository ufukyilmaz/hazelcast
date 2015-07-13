package com.hazelcast.session.license;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Tomcat7Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.catalina.LifecycleException;
import org.hamcrest.Matcher;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class Tomcat7InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat7Configurator("hazelcast-without-license.xml", "hazelcast-client-without-license.xml");
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
