package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;

public class InvocationFutureAccessor {

    // InvocationFuture cannot be mocked reliably. This class prepares a dummy InvocationFuture
    // suitable for explicit completion by test code where a mock would be used.
    public static InvocationFuture dummyInvocationFuture() throws UnknownHostException {
        Invocation.Context context = new Invocation.Context(
            null, null, null, null, null, 250L, null, null,
                Logger.getLogger(InvocationFutureAccessor.class),
                null, null, null, null, null, null, null, null, null, null
        );
        Invocation invocation = new TargetInvocation(context, new DummyOperation(),
                new Address("127.0.0.1", 5701), 250, 10L, 10000L, false);

        return new InvocationFuture(invocation, false);
    }

}
