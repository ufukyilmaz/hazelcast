package com.hazelcast.security.impl;

import com.hazelcast.impl.Node;

abstract class SecureProxySupport {

	final Node node;

	SecureProxySupport(Node node) {
		super();
		this.node = node;
	}
	
//	void checkPermission(Permission p) throws AccessControlException {
//		SecurityUtil.checkPermission(node.securityContext, p);
//	}
	
}
