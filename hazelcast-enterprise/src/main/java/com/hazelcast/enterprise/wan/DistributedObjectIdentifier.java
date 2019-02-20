package com.hazelcast.enterprise.wan;

/**
 * Identifier for a distributed object participating in WAN replication.
 * In addition to the service name and the object name, the identifier
 * carries the total backup count for the distributed object. The backup
 * count may be calculated from the configuration but this prevents cache
 * lookups.
 */
public class DistributedObjectIdentifier {
    private final String serviceName;
    private final String objectName;
    private final int totalBackupCount;

    public DistributedObjectIdentifier(String serviceName, String objectName, int totalBackupCount) {
        this.serviceName = serviceName;
        this.objectName = objectName;
        this.totalBackupCount = totalBackupCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistributedObjectIdentifier that = (DistributedObjectIdentifier) o;

        if (totalBackupCount != that.totalBackupCount) {
            return false;
        }
        if (!serviceName.equals(that.serviceName)) {
            return false;
        }
        return objectName.equals(that.objectName);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getObjectName() {
        return objectName;
    }

    public int getTotalBackupCount() {
        return totalBackupCount;
    }

    @Override
    public int hashCode() {
        int result = serviceName.hashCode();
        result = 31 * result + objectName.hashCode();
        result = 31 * result + totalBackupCount;
        return result;
    }

    @Override
    public String toString() {
        return "DistributedObjectIdentifier{"
                + "serviceName='" + serviceName + '\''
                + ", objectName='" + objectName + '\''
                + ", totalBackupCount=" + totalBackupCount
                + '}';
    }
}
