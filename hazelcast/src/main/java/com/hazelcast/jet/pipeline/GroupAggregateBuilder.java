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
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.Map;

public interface GroupAggregateBuilder<K, R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    <T, R> Tag<R> add(
            @Nonnull BatchStageWithKey<T, K> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object and emits a {@code
     * Map.Entry(key, resultsByTag)} for each distinct key. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag}. Use the tags you got from this
     * builder to access the results.
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    BatchStage<Map.Entry<K, ItemsByTag>> build();
}
