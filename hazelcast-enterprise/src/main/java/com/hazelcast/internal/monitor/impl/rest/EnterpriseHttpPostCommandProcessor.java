package com.hazelcast.internal.monitor.impl.rest;

import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.internal.monitor.LicenseInfo;

/**
 * Enterprise override for HTTP POST command processor.
 */
class EnterpriseHttpPostCommandProcessor extends HttpPostCommandProcessor {

    EnterpriseHttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    protected String responseOnSetLicenseSuccess() {
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) textCommandService.getNode().getNodeExtension();
        LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());
        return response(ResponseType.SUCCESS, "licenseInfo", licenseInfo.toJson(), "message",
                "License updated at run time - please make sure to update the license in the persistent"
                        + " configuration to avoid losing the changes on restart.");
    }

}
