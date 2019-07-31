package com.hazelcast.enterprise.wan.impl.operation;

/**
 * Response status for WAN protocol negotiation between source and target cluster.
 *
 * @see WanProtocolNegotiationResponse
 */
public enum WanProtocolNegotiationStatus {
    /**
     * OK response meaning protocol was successfully established.
     */
    OK((byte) 0),

    /**
     * Failure response meaning protocol was not established because the source
     * cluster is misconfigured with a wrong target group name.
     */
    GROUP_NAME_MISMATCH((byte) 1),

    /**
     * Failure response meaning protocol was not established because there was no
     * matching WAN protocol supported by both the source and target cluster.
     */
    PROTOCOL_MISMATCH((byte) 2),

    /**
     * Failure response meaning protocol was not established but the cause is
     * unknown. This may be set when the target cluster is a newer cluster than
     * the source cluster and the target cluster sends a newer failure
     * negotiation status which is not known by the source cluster.
     */
    UNKNOWN((byte) -1);

    private static final WanProtocolNegotiationStatus[] STATE_VALUES = values();

    private final byte statusCode;

    WanProtocolNegotiationStatus(byte statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * Returns the WanPublisherState as an enum.
     */
    public static WanProtocolNegotiationStatus getByType(final byte statusCode) {
        for (WanProtocolNegotiationStatus state : STATE_VALUES) {
            if (state.statusCode == statusCode) {
                return state;
            }
        }
        return UNKNOWN;
    }

    public byte getStatusCode() {
        return statusCode;
    }
}
