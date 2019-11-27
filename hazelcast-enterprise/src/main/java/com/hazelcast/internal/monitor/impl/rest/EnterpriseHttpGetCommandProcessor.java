package com.hazelcast.internal.monitor.impl.rest;

import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.LicenseInfo;

/**
 * Enterprise override for HTTP GET command processor. It handles the license info requests.
 */
class EnterpriseHttpGetCommandProcessor
        extends HttpGetCommandProcessor {

    EnterpriseHttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    protected void handleLicense(HttpGetCommand command) {
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) textCommandService.getNode().getNodeExtension();
        LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());

        prepareResponse(command, new JsonObject().add("licenseInfo", licenseInfo.toJson()));
    }
}
