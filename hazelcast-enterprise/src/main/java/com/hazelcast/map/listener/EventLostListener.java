package com.hazelcast.map.listener;

import com.hazelcast.map.EventLostEvent;

/**
 * Invoked upon lost of event or events.
 *
 * @since 3.5
 */
public interface EventLostListener extends MapListener {

    /**
     * Invoked upon lost of event or events.
     */
    void eventLost(EventLostEvent event);
}
