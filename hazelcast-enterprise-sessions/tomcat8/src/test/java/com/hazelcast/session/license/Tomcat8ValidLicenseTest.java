package com.hazelcast.session.license;

import com.hazelcast.session.Java6ExcludeRule;
import com.hazelcast.session.Tomcat8Configurator;
import com.hazelcast.session.WebContainerConfigurator;
import org.junit.Rule;

public class Tomcat8ValidLicenseTest extends AbstractValidLicenseTest {

    @Rule
    public Java6ExcludeRule java6ExcludeRule = new Java6ExcludeRule();

    @Override
    protected WebContainerConfigurator<?> getWebContainerConfigurator() {
        return new Tomcat8Configurator("hazelcast-with-valid-license.xml", "hazelcast-client-with-valid-license.xml");
    }
}
