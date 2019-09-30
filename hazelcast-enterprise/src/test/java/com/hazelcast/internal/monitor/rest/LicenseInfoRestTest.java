package com.hazelcast.internal.monitor.rest;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseInfoRestTest {

    private Config config = new Config();

    @Before
    public void setup() {
        config.setProperty(GroupProperty.REST_ENABLED.getName(), "true");
    }

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testGetLicenseInfo() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator1 = new HTTPCommunicator(instance);

        HazelcastInstanceImpl instanceImpl = ((HazelcastInstanceProxy) instance).getOriginal();
        License expected = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instanceImpl.node.getBuildInfo().getVersion());

        JsonObject actual = Json.parse(communicator1.getLicenseInfo().replace("licenseInfo", "")).asObject();

        assertEquals(expected.getAllowedNumberOfNodes(), actual.getInt("maxNodeCount", 0));
        assertEquals(MILLISECONDS.toDays(expected.getExpiryDate().getTime()),
                MILLISECONDS.toDays(actual.getLong("expiryDate", 0)));
        assertEquals(expected.getType().getCode(), actual.getInt("type", -1));

        assertJsonStringEqualsOrNull(expected.getEmail(), actual.get("ownerEmail"));
        assertJsonStringEqualsOrNull(expected.getCompanyName(), actual.get("companyName"));
    }

    private void assertJsonStringEqualsOrNull(String expected, JsonValue actual) {
        if (actual.isNull()) {
            assertNull(expected);
        } else {
            assertEquals(expected, actual.asString());
        }
    }

}
