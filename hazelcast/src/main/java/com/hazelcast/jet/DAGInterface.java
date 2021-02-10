package com.hazelcast.jet;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.LOCAL_PARALLELISM_USE_DEFAULT;

public interface DAGInterface {

    /**
     * Returns a DOT format (graphviz) representation of the DAG.
     */
    @Nonnull
    default String toDotString() {
        return toDotString(LOCAL_PARALLELISM_USE_DEFAULT);
    }

    /**
     * Returns a DOT format (graphviz) representation of the DAG and annotates
     * the vertices using default parallelism with the supplied value.
     */
    @Nonnull
    String toDotString(int defaultParallelism);
}
