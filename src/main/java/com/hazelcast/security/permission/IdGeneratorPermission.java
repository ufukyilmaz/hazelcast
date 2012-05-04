package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*; 

public class IdGeneratorPermission extends InstancePermission {
	
	private final static int INCREMENT 		= 0x4;
	private final static int ALL 			= CREATE | DESTROY | INCREMENT ;

	public IdGeneratorPermission(String name, String... actions) {
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
			} else if(ACTION_INCREMENT.equals(actions[i])) {
				mask |= INCREMENT;
			} else if(ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			}
		}
		return mask;
	}
}
