/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Represents the definition of a sliding window.
 *
 * @since 3.0
 */
public class SlidingWindowDefinition extends WindowDefinition {

    private long windowSize;
    private long slideBy;

    SlidingWindowDefinition() {
    }

    SlidingWindowDefinition(long windowSize, long slideBy) {
        checkPositive(windowSize, "windowSize must be positive");
        checkPositive(slideBy, "slideBy must be positive");
        checkTrue(windowSize % slideBy == 0, "windowSize must be integer multiple of slideBy, mod("
                + windowSize + ", " + slideBy + ") != 0");
        this.windowSize = windowSize;
        this.slideBy = slideBy;
    }

    @Override
    public SlidingWindowDefinition setEarlyResultsPeriod(long earlyResultPeriodMs) {
        return (SlidingWindowDefinition) super.setEarlyResultsPeriod(earlyResultPeriodMs);
    }

    /**
     * Returns the length of the window (the size of the timestamp range it
     * covers). It is an integer multiple of {@link #slideBy()}.
     */
    public long windowSize() {
        return windowSize;
    }

    /**
     * Returns the size of the sliding step.
     */
    public long slideBy() {
        return slideBy;
    }

    @Override
    public int getClassId() {
        return JetPipelineDataSerializerHook.SLIDING_WINDOW_DEFINITION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(windowSize);
        out.writeLong(slideBy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        windowSize = in.readLong();
        slideBy = in.readLong();
    }
}
