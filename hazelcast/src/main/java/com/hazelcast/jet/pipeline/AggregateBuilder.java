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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;

public interface AggregateBuilder<R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder was
     * obtained from. Use it to get the results for this stage from the
     * {@code ItemsByTag} appearing in the output of the stage you are building.
     */
    @Nonnull
    Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to get the results
     * for this stage from the {@code ItemsByTag} appearing in the output of
     * the stage you are building.
     */
    @Nonnull
    <T, R> Tag<R> add(
            @Nonnull BatchStage<T> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag} and the {@code finishFn} you supply
     * transforms it to the final result to emit. Use the tags you got from
     * this builder in the implementation of {@code finishFn} to access the
     * results.
     *
     * @param finishFn the finishing function for the composite aggregate
     *     operation. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <R> the output item type
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    <R> BatchStage<R> build(
            @Nonnull FunctionEx<? super ItemsByTag, ? extends R> finishFn
    );

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag}. Use the tags you got from
     * this builder to access the results.
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    BatchStage<ItemsByTag> build();

}
