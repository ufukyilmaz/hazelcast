package com.hazelcast.internal.monitor.rest;

import com.hazelcast.internal.monitor.LicenseInfo;
import com.hazelcast.internal.monitor.impl.rest.LicenseInfoImpl;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LicenseInfoTest {

    @Test
    public void test() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(new Date());
        when(license.getType()).thenReturn(LicenseType.ENTERPRISE_HD);
        when(license.getAllowedNumberOfNodes()).thenReturn(4);
        when(license.getCompanyName()).thenReturn("SuperCompany");
        when(license.getEmail()).thenReturn("SuperCompanyOwnerEmail");
        when(license.computeKeyHash()).thenReturn("SomeKeyHash");

        LicenseInfo actual = new LicenseInfoImpl(license);
        JsonObject json = actual.toJson();

        LicenseInfo expected = new LicenseInfoImpl();
        expected.fromJson(json);

        assertEquals(expected, actual);
        assertEquals(actual.getMaxNodeCountAllowed(), license.getAllowedNumberOfNodes());
        assertEquals(actual.getExpirationTime(), license.getExpiryDate().getTime());
        assertEquals(actual.getType(), license.getType());
        assertEquals(actual.getCompanyName(), license.getCompanyName());
        assertEquals(actual.getOwnerEmail(), license.getEmail());
        assertEquals(actual.getKeyHash(), license.computeKeyHash());
    }

}
