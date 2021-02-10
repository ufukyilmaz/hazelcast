package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;

public interface AggregateBuilder1<T0> {

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
    public <T> Tag<T> add(@Nonnull BatchStage<T> stage);

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of pipeline stages registered with this builder object. The tags you
     * register with the aggregate operation must match the tags you registered
     * with this builder. Refer to the documentation on {@link
     * BatchStage#aggregateBuilder()} for more details.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the output item
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <R> BatchStage<R> build(@Nonnull AggregateOperation<?, R> aggrOp);
}
