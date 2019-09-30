package com.hazelcast.internal.monitor.impl.rest;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_GET;

public class EnterpriseTextCommandServiceImpl
        extends TextCommandServiceImpl {

    public EnterpriseTextCommandServiceImpl(Node node) {
        super(node);
        register(HTTP_GET, new EnterpriseHttpGetCommandProcessor(this));
    }

}
