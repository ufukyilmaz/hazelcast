package com.hazelcast.cache.enterprise.impl;

import com.hazelcast.cache.enterprise.EnterpriseCacheRecordStore;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

/**
 * @author sozal 14/10/14
 */
public abstract class AbstractEnterpriseCacheRecordStore implements EnterpriseCacheRecordStore {

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
