package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;

public interface WindowGroupAggregateBuilder1<T0, K> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<T0> tag0();

    /**
     * Adds another stage that will contribute its data to the windowed
     * group-and-aggregate stage being constructed. Returns the tag you'll use
     * to refer to this stage when building the {@code AggregateOperation} that
     * you'll pass to {@link #build build()}.
     */
    @Nonnull
    public <T> Tag<T> add(@Nonnull StreamStageWithKey<T, K> stage);

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * cogroup-and-aggregate of the pipeline stages registered with this builder object.
     * The tags you register with the aggregate operation must match the tags
     * you registered with this builder.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp        the aggregate operation to perform
     * @param <R>           the type of the aggregation result
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <R> StreamStage<KeyedWindowResult<K, R>> build(@Nonnull AggregateOperation<?, ? extends R> aggrOp);
}
