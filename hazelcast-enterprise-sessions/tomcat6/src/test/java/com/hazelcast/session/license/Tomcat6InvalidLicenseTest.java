package com.hazelcast.session.license;

import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.Tomcat6Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;


public class Tomcat6InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat6Configurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @Override
    protected Matcher<? extends Throwable> getCause() {
        return new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object o) {
                return o == null;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    @Override
    protected Class<? extends Throwable> getException() {
        return InvalidLicenseException.class;
    }
}
