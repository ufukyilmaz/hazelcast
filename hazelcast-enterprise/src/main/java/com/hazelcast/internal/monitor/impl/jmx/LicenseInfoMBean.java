package com.hazelcast.internal.monitor.impl.jmx;

import com.hazelcast.internal.monitor.LicenseInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.jmx.HazelcastMBean;
import com.hazelcast.internal.jmx.ManagedAnnotation;
import com.hazelcast.internal.jmx.ManagedDescription;
import com.hazelcast.internal.jmx.ManagementService;

import java.util.Map;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

@ManagedDescription("HazelcastInstance.LicenseInfo")
public class LicenseInfoMBean
        extends HazelcastMBean<LicenseInfo> {

    LicenseInfoMBean(LicenseInfo licenseInfo, Node node, ManagementService service) {
        super(licenseInfo, service);

        final Map<String, String> properties = createHashMap(2);
        properties.put("type", quote("HazelcastInstance.LicenseInfo"));
        properties.put("name", quote("node" + node.address));
        properties.put("instance", quote(node.nodeEngine.getHazelcastInstance().getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("maxNodeCountAllowed")
    @ManagedDescription("Maximum nodes allowed to form a cluster under the current license")
    public String getMaxNodeCountAllowed() {
        return String.valueOf(managedObject.getMaxNodeCountAllowed());
    }

    @ManagedAnnotation("expiryDate")
    @ManagedDescription("The expiry date of the current license")
    public String getExpiryDate() {
        return String.valueOf(managedObject.getExpirationTime());
    }

    @ManagedAnnotation("typeCode")
    @ManagedDescription("The type code of the current license")
    public String getTypeCode() {
        return String.valueOf(managedObject.getType().getCode());
    }

    @ManagedAnnotation("type")
    @ManagedDescription("The type of the current license")
    public String getType() {
        return managedObject.getType().getText();
    }

    @ManagedAnnotation("ownerEmail")
    @ManagedDescription("The email of the owner on the current license")
    public String getOwnerEmail() {
        return managedObject.getOwnerEmail();
    }

    @ManagedAnnotation("companyName")
    @ManagedDescription("The name of the company on the current license")
    public String getCompanyName() {
        return managedObject.getCompanyName();
    }

    @ManagedAnnotation("keyHash")
    @ManagedDescription("SHA-256 hash of license key of the current license as a Base64 encoded string")
    public String getKeyHash() {
        return managedObject.getKeyHash();
    }
}
