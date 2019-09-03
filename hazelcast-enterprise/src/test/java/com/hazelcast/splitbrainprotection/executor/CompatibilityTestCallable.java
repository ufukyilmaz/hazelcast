package com.hazelcast.splitbrainprotection.executor;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class CompatibilityTestCallable implements Callable<String>, Serializable {
    public static final String RESPONSE = "response";

    private static final long serialVersionUID = 1L;

    @Override
    public String call() {
        return RESPONSE;
    }

    public void run() {
    }
}
