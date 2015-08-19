package com.hazelcast.spi.hotrestart;

import com.hazelcast.core.HazelcastException;

/**
 * @author mdogan 06/01/15
 */
public class HotRestartException extends HazelcastException {

    public HotRestartException() {
    }

    public HotRestartException(String message) {
        super(message);
    }

    public HotRestartException(String message, Throwable cause) {
        super(message, cause);
    }

    public HotRestartException(Throwable cause) {
        super(cause);
    }
}

