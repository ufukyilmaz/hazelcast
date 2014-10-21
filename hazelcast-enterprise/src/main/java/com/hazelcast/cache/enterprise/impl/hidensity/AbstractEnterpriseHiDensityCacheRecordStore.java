package com.hazelcast.cache.enterprise.impl.hidensity;

import com.hazelcast.cache.enterprise.hidensity.EnterpriseHiDensityCacheRecord;
import com.hazelcast.cache.enterprise.hidensity.EnterpriseHiDensityCacheRecordMap;
import com.hazelcast.cache.enterprise.hidensity.EnterpriseHiDensityCacheRecordStore;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

/**
 * @author sozal 18/10/14
 */
public abstract class AbstractEnterpriseHiDensityCacheRecordStore<CRM extends EnterpriseHiDensityCacheRecordMap<? extends Data, ? extends EnterpriseHiDensityCacheRecord>>
        extends AbstractCacheRecordStore<CRM>
        implements EnterpriseHiDensityCacheRecordStore {

    public AbstractEnterpriseHiDensityCacheRecordStore(String name,
                                                       int partitionId,
                                                       NodeEngine nodeEngine,
                                                       AbstractCacheService cacheService,
                                                       EnterpriseSerializationService serializationService,
                                                       ExpiryPolicy expiryPolicy) {
        super(name,
              partitionId,
              nodeEngine,
              cacheService,
              serializationService,
              null,
              expiryPolicy);
    }

    public AbstractEnterpriseHiDensityCacheRecordStore(String name,
                                                       int partitionId,
                                                       NodeEngine nodeEngine,
                                                       AbstractCacheService cacheService,
                                                       EnterpriseSerializationService serializationService,
                                                       CacheRecordFactory cacheRecordFactory,
                                                       ExpiryPolicy expiryPolicy) {
        super(name,
              partitionId,
              nodeEngine,
              cacheService,
              serializationService,
              cacheRecordFactory,
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
