package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.serialization.Data;

import java.util.Map;

/**
 * Map storage interface to hold {@link HiDensityRecord} objects.
 *
 * @param <R> Type of the Hi-Density record stored in this record map.
 */
public interface HiDensityRecordMap<R extends HiDensityRecord>
        extends Map<Data, R> {
}
