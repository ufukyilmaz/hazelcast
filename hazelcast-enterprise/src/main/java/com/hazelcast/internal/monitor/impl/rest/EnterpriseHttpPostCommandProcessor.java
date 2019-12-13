package com.hazelcast.internal.monitor.impl.rest;

import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpBadRequestException;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.operation.SetLicenseOperation;
import com.hazelcast.internal.monitor.LicenseInfo;
import com.hazelcast.license.exception.InvalidLicenseException;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

/**
 * Enterprise override for HTTP POST command processor.
 */
class EnterpriseHttpPostCommandProcessor extends HttpPostCommandProcessor {

    EnterpriseHttpPostCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    protected void handleSetLicense(HttpPostCommand cmd) throws Throwable {
        String[] params = decodeParamsAndAuthenticate(cmd, 3);
        try {
            final int retryCount = 100;
            // assumes that both groupName and password are present
            String licenseKey = params[2];
            invokeOnStableClusterSerial(getNode().nodeEngine,
                    () -> new SetLicenseOperation(licenseKey), retryCount).get();
            prepareResponse(cmd, responseOnSetLicenseSuccess());
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof InvalidLicenseException) {
                throw new HttpBadRequestException(cause.getMessage());
            }
            throw cause;
        }
    }

    private JsonObject responseOnSetLicenseSuccess() {
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode().getNodeExtension();
        LicenseInfo licenseInfo = new LicenseInfoImpl(nodeExtension.getLicense());
        JsonObject jsonResponse = response(ResponseType.SUCCESS, "message",
                "License updated at run time - please make sure to update the license in the persistent"
                        + " configuration to avoid losing the changes on restart.");
        jsonResponse.add("licenseInfo", licenseInfo.toJson());
        return jsonResponse;
    }

}
