package com.hazelcast.enterprise.monitor.impl.rest;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_GET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_POST;

public class EnterpriseTextCommandServiceImpl
        extends TextCommandServiceImpl {

    public EnterpriseTextCommandServiceImpl(Node node) {
        super(node);
        register(HTTP_GET, new EnterpriseHttpGetCommandProcessor(this));
        register(HTTP_POST, new EnterpriseHttpPostCommandProcessor(this));
    }

}
