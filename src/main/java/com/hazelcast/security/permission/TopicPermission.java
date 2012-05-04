package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*;

public class TopicPermission extends InstancePermission {
	
	private final static int PUBLISH 		= 0x4;
	private final static int LISTEN 		= 0x8;
	private final static int STATS	 		= 0x16;
	private final static int ALL 			= CREATE | DESTROY | LISTEN | PUBLISH | STATS;

	public TopicPermission(String name, String... actions) {
		super(name, actions);
	}

	protected int initMask(String[] actions) {
		int mask = NONE;
		for (int i = 0; i < actions.length; i++) {
			if(ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(ACTION_PUBLISH.equals(actions[i])) {
				mask |= PUBLISH;
			} else if(ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			} else if(ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}
		}
		return mask;
	}
}
