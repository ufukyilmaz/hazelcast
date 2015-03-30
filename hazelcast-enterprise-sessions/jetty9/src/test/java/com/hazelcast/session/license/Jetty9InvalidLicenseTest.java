package com.hazelcast.session.license;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.session.JettyConfigurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;


public class Jetty9InvalidLicenseTest extends AbstractInvalidLicenseTest {

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new JettyConfigurator("hazelcast-without-license.xml","hazelcast-client-without-license.xml");
    }

    @After
    @Override
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        instance1.stop();
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
