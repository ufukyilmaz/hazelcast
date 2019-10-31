package com.hazelcast.internal.monitor;

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.license.domain.LicenseType;

/**
 * DTO object to carry license information to MC
 */
public interface LicenseInfo
        extends JsonSerializable {

    /**
     * @return the license type ({@link LicenseType} associated with the license key assigned on this node
     */
    LicenseType getType();

    /**
     * @return the maximum number of nodes in the cluster, allowed for the license key assigned on this node
     */
    int getMaxNodeCountAllowed();

    /**
     * @return the expiration time in milliseconds since epoch, of the license key assigned on this node
     */
    long getExpirationTime();

    /**
     * @return the company name to whom the license corresponds, based on the license key assigned on this node
     */
    String getCompanyName();

    /**
     * @return the owner email, of the license key assigned on this node
     */
    String getOwnerEmail();

    /**
     * @return hash of the license key assigned on this node, computed as SHA-256 hash of the key encoded as a Base64
     * string
     */
    String getKeyHash();
}
