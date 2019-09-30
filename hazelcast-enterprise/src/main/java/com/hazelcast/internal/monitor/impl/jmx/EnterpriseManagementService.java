package com.hazelcast.internal.monitor.impl.jmx;

import com.hazelcast.internal.monitor.LicenseInfo;
import com.hazelcast.internal.monitor.impl.rest.LicenseInfoImpl;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.jmx.InstanceMBean;
import com.hazelcast.internal.jmx.ManagementService;

/**
 * JMX Extension point for EE
 * Offers additional JMX beans to the service for the Enterprise version of Hazelcast
 */
public class EnterpriseManagementService
        extends ManagementService {

    public EnterpriseManagementService(HazelcastInstanceImpl instance) {
        super(instance);
    }

    @Override
    protected InstanceMBean createInstanceMBean(HazelcastInstanceImpl instance) {
        return new EnterpriseInstanceMBean(instance, this);
    }

    private static class EnterpriseInstanceMBean
            extends InstanceMBean {

        private LicenseInfoMBean licenseInfoMBean;

        EnterpriseInstanceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
            super(hazelcastInstance, managementService);
            createAndRegisterLicenseMBean(hazelcastInstance, managementService);
        }

        private void createAndRegisterLicenseMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
            EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) hazelcastInstance.node.getNodeExtension();
            LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());
            this.licenseInfoMBean = new LicenseInfoMBean(licenseInfo, hazelcastInstance.node, managementService);
            register(licenseInfoMBean);
        }

    }

}
