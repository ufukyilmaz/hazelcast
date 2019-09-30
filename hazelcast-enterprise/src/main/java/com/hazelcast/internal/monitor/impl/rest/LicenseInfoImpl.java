package com.hazelcast.internal.monitor.impl.rest;

import com.hazelcast.internal.monitor.LicenseInfo;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseType;

public class LicenseInfoImpl
        implements LicenseInfo {

    private long expirationTime;
    private int maxNodeCount;
    private LicenseType type;
    private String companyName;
    private String ownerEmail;
    private String keyHash;

    public LicenseInfoImpl() {
    }

    public LicenseInfoImpl(final License license) {
        this(license.getExpiryDate().getTime(), license.getAllowedNumberOfNodes(),
                license.getType(), license.getCompanyName(), license.getEmail(),
                license.computeKeyHash());
    }

    private LicenseInfoImpl(long expirationTime, int maxNodeCount, LicenseType type, String companyName,
                            String ownerEmail, String keyHash) {
        this.expirationTime = expirationTime;
        this.maxNodeCount = maxNodeCount;
        this.type = type;
        this.companyName = companyName;
        this.ownerEmail = ownerEmail;
        this.keyHash = keyHash;
    }

    @Override
    public LicenseType getType() {
        return type;
    }

    @Override
    public int getMaxNodeCountAllowed() {
        return maxNodeCount;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public String getOwnerEmail() {
        return ownerEmail;
    }

    @Override
    public String getCompanyName() {
        return companyName;
    }

    @Override
    public String getKeyHash() {
        return keyHash;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("expiryDate", expirationTime);
        json.add("maxNodeCount", maxNodeCount);
        json.add("type", type != null ? type.getCode() : -1);
        json.add("companyName", companyName);
        json.add("ownerEmail", ownerEmail);
        json.add("keyHash", keyHash);
        return json;
    }

    @Override
    public void fromJson(JsonObject json) {
        this.expirationTime = json.getLong("expiryDate", 0);
        this.maxNodeCount = json.getInt("maxNodeCount", 0);
        this.ownerEmail = json.getString("ownerEmail", null);
        this.companyName = json.getString("companyName", null);
        this.type = LicenseType.getLicenseType(
                json.getInt("type", LicenseType.getDefault().getCode()));
        this.keyHash = json.getString("keyHash", null);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LicenseInfoImpl that = (LicenseInfoImpl) o;

        if (expirationTime != that.expirationTime) {
            return false;
        }
        if (maxNodeCount != that.maxNodeCount) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (companyName != null ? !companyName.equals(that.companyName) : that.companyName != null) {
            return false;
        }
        if (ownerEmail != null ? !ownerEmail.equals(that.ownerEmail) : that.ownerEmail != null) {
            return false;
        }

        return keyHash != null ? keyHash.equals(that.keyHash) : that.keyHash == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + maxNodeCount;
        result = 31 * result + type.hashCode();
        result = 31 * result + (companyName != null ? companyName.hashCode() : 0);
        result = 31 * result + (ownerEmail != null ? ownerEmail.hashCode() : 0);
        result = 31 * result + (keyHash != null ? keyHash.hashCode() : 0);
        return result;
    }
}
