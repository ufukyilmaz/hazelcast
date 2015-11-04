/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.util.concurrent;


import java.util.concurrent.locks.LockSupport;

/**
 * Idling strategy for threads when they have no work to do.
 * <p/>
 * Spin for maxSpins, then
 * {@link Thread#yield()} for maxYields, then
 * {@link java.util.concurrent.locks.LockSupport#parkNanos(long)} on an exponential backoff to maxParkPeriodNs
 */
public class BackoffIdleStrategy implements IdleStrategy {
    /** Possible states of the loop employing a backoff strategy */
    @SuppressWarnings("checkstyle:javadocvariable")
    public enum State {
        NOT_IDLE, SPINNING, YIELDING, PARKING
    }

    private final long maxSpins;
    private final long maxYields;
    private final long minParkPeriodNs;
    private final long maxParkPeriodNs;

    private State state;

    private long spins;
    private long yields;
    private long parkPeriodNs;

    /**
     * Create a set of state tracking idle behavior
     *
     * @param maxSpins        to perform before moving to {@link Thread#yield()}
     * @param maxYields       to perform before moving to {@link java.util.concurrent.locks.LockSupport#parkNanos(long)}
     * @param minParkPeriodNs to use when initiating parking
     * @param maxParkPeriodNs to use when parking
     */
    public BackoffIdleStrategy(long maxSpins, long maxYields, long minParkPeriodNs, long maxParkPeriodNs) {
        this.maxSpins = maxSpins;
        this.maxYields = maxYields;
        this.minParkPeriodNs = minParkPeriodNs;
        this.maxParkPeriodNs = maxParkPeriodNs;
        this.state = State.NOT_IDLE;
    }

    /**
     * {@inheritDoc}
     */
    public boolean idle(final int workCount) {
        if (workCount > 0) {
            spins = 0;
            yields = 0;
            state = State.NOT_IDLE;
            return false;
        }

        switch (state) {
            case NOT_IDLE:
                state = State.SPINNING;
                spins++;
                break;

            case SPINNING:
                if (++spins > maxSpins) {
                    state = State.YIELDING;
                    yields = 0;
                }
                break;

            case YIELDING:
                if (++yields > maxYields) {
                    state = State.PARKING;
                    parkPeriodNs = minParkPeriodNs;
                } else {
                    Thread.yield();
                }
                break;

            case PARKING:
                LockSupport.parkNanos(parkPeriodNs);
                parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs);
                break;

            default:
                throw new RuntimeException("enum member unaccounted for");
        }
        return state == State.PARKING && parkPeriodNs == maxParkPeriodNs;
    }
}

