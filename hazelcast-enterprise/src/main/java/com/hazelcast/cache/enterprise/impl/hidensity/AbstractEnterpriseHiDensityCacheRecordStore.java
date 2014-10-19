package com.hazelcast.cache.enterprise.impl.hidensity;

import com.hazelcast.cache.enterprise.hidensity.EnterpriseHiDensityCacheRecordStore;
import com.hazelcast.cache.enterprise.impl.AbstractEnterpriseCacheRecordStore;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

/**
 * @author sozal 18/10/14
 */
public abstract class AbstractEnterpriseHiDensityCacheRecordStore
        extends AbstractEnterpriseCacheRecordStore
        implements EnterpriseHiDensityCacheRecordStore {

    protected long expiryPolicyToTTL(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            return -1;
        }
        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            return -1;
        }
        long durationAmount = expiryDuration.getDurationAmount();
        TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
        return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
    }

    protected ExpiryPolicy ttlToExpirePolicy(long ttl) {
        return new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));
    }

}
