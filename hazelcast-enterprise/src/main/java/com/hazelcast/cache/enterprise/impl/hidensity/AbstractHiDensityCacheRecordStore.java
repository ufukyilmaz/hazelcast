package com.hazelcast.cache.enterprise.impl.hidensity;

import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecordMap;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

/**
 * @author sozal 18/10/14
 */
public abstract class AbstractHiDensityCacheRecordStore<
            R extends HiDensityCacheRecord,
            CRM extends HiDensityCacheRecordMap<? extends Data, R>>
        extends AbstractCacheRecordStore<R, CRM>
        implements HiDensityCacheRecordStore {

    public AbstractHiDensityCacheRecordStore(String name,
                                             int partitionId,
                                             NodeEngine nodeEngine,
                                             AbstractCacheService cacheService,
                                             ExpiryPolicy expiryPolicy) {
        super(name,
              partitionId,
              nodeEngine,
              cacheService,
              expiryPolicy);
    }

    protected long expiryPolicyToTTL(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            return -1;
        }
        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return -1;
        }
    }

    protected ExpiryPolicy ttlToExpirePolicy(long ttl) {
        return new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));
    }

}
