package com.hazelcast.spi.hotrestart;

import com.hazelcast.core.HazelcastException;

/**
 * Thrown when force start request is detected during Hot Restart
 * member join, validation or data load phases.
 */
public class ForceStartException extends HazelcastException {
}
