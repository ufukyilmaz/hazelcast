package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;

public interface StreamHashJoinBuilder<T0> extends GeneralHashJoinBuilder<T0> {

    /**
     * Builds a new pipeline stage that performs the hash-join operation. Attaches
     * the stage to all the contributing stages.
     *
     * @param mapToOutputFn the function to map the output item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @return the new hash-join pipeline stage
     */
    <R> StreamStage<R> build(BiFunctionEx<T0, ItemsByTag, R> mapToOutputFn);

}
