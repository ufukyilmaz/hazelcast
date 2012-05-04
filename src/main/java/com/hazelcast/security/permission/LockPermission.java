package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*;

public class LockPermission extends InstancePermission {
	
	private final static int LOCK	 		= 0x4;
	private final static int STATS	 		= 0x8;
	private final static int ALL 			= CREATE | DESTROY | LOCK | STATS;

	public LockPermission(Object key, String... actions) {
		super(String.valueOf(key), actions);
	}

	protected int initMask(String[] actions) {
		int mask = NONE;
		for (int i = 0; i < actions.length; i++) {
			if(ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ACTION_LOCK.equals(actions[i])) {
				mask |= LOCK;
			} else if(ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}  
		}
		return mask;
	}
}
