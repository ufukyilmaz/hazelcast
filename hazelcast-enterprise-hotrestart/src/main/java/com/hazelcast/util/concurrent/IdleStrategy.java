/*
 * Original work Copyright 2014 Real Logic Ltd.
 * Modified work Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Idle strategy for use by threads when then do not have work to do.
 */
public interface IdleStrategy {
    /**
     * Perform current idle strategy (or not) depending on whether work has been done or not
     *
     * @param workCount performed in last duty cycle.
     * @return whether the strategy has reached the longest pause time
     */
    boolean idle(int workCount);
}
