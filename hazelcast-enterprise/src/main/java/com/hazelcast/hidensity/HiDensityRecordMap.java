package com.hazelcast.hidensity;

import com.hazelcast.nio.serialization.Data;

import java.util.Map;

/**
 * Map storage interface to hold {@link HiDensityRecord} objects.
 *
 * @param <R> Type of the hi-density record stored in this record map
 *
 * @author sozal 18/02/15
 */
public interface HiDensityRecordMap<R extends HiDensityRecord>
        extends Map<Data, R> {

}
