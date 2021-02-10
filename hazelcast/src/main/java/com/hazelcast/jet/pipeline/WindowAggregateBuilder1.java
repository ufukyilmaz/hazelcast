package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.WindowResult;

import javax.annotation.Nonnull;

public interface WindowAggregateBuilder1<T0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<T0> tag0();

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    public <E> Tag<E> add(StreamStage<E> stage);

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * co-aggregation of the pipeline stages registered with this builder
     * object. The tags you register with the aggregate operation must match
     * the tags you registered with this builder.
     *
     * @param aggrOp        the aggregate operation to perform
     * @param <R>           the type of the aggregation result
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <A, R> StreamStage<WindowResult<R>> build(@Nonnull AggregateOperation<A, R> aggrOp);
}
