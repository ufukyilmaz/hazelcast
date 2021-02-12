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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;

public interface WindowGroupAggregateBuilder<K, R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder was
     * obtained from. Use this tag to refer to this stage when extracting the
     * results from the aggregated stage.
     */
    @Nonnull
    Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the windowed
     * group-and-aggregate stage being constructed. Returns the tag you'll use
     * to refer to this stage when building the {@code AggregateOperation} that
     * you'll pass to {@link #build build()}.
     */
    @Nonnull
    <T, R> Tag<R> add(
            @Nonnull StreamStageWithKey<T, K> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * cogroup-and-aggregate operation on the stages registered with this
     * builder object. The composite aggregate operation places the results of
     * the individual aggregate operations in an {@code ItemsByTag}. Use the
     * tags you got from this builder to access the results.
     */
    @Nonnull
    StreamStage<KeyedWindowResult<K, ItemsByTag>> build();
}
