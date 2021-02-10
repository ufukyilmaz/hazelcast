package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.WindowResult;

import javax.annotation.Nonnull;

public interface WindowAggregateBuilder<R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    public <T, R> Tag<R> add(
            StreamStage<T> stage,
            AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * co-aggregation of the stages registered with this builder object. The
     * composite aggregate operation places the results of the individual
     * aggregate operations in an {@code ItemsByTag}.
     *
     * @return a new stage representing the cogroup-and-aggregate operation
     */
    @Nonnull
    public StreamStage<WindowResult<ItemsByTag>> build();

}
