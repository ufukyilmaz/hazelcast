package com.hazelcast.security;

import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.security.permission.*;
import com.hazelcast.util.AddressUtil;

public final class SecurityUtil {
	
	private static final ThreadLocal<Boolean> SECURE_CALL = new ThreadLocal<Boolean>() ;
	
	static void setSecureCall() {
		if(isSecureCall()) {
			throw new SecurityException("Not allowed! <SECURE_CALL> flag is already set!");
		}
		SECURE_CALL.set(Boolean.TRUE);
	}
	
	static void resetSecureCall() {
		if(!isSecureCall()) {
			throw new SecurityException("Not allowed! <SECURE_CALL> flag is not set!");
		}
		SECURE_CALL.remove();
	}
	
	private static boolean isSecureCall() {
		final Boolean value = SECURE_CALL.get();
		return value != null && value.booleanValue();
	}
	
	public static ClusterPermission createPermission(PermissionConfig permissionConfig) {
		final String[] actions = permissionConfig.getActions().toArray(new String[0]);
		switch (permissionConfig.getType()) {
		case MAP:
			return new MapPermission(permissionConfig.getName(), actions);

		case QUEUE:
			return new QueuePermission(permissionConfig.getName(), actions);
			
		case ATOMIC_LONG:
			return new AtomicLongPermission(permissionConfig.getName(), actions);
			
		case COUNTDOWN_LATCH:
			return new CountDownLatchPermission(permissionConfig.getName(), actions);
			
		case EXECUTOR_SERVICE:
			return new ExecutorServicePermission(permissionConfig.getName(), actions);
			
		case LIST:
			return new ListPermission(permissionConfig.getName(), actions);
			
		case LOCK:
			return new LockPermission(permissionConfig.getName(), actions);
		
		case MULTIMAP:
			return new MultiMapPermission(permissionConfig.getName(), actions);
			
		case SEMAPHORE:
			return new SemaphorePermission(permissionConfig.getName(), actions);
			
		case SET: 
			return new SetPermission(permissionConfig.getName(), actions);
			
		case TOPIC:
			return new TopicPermission(permissionConfig.getName(), actions);
			
		case ID_GENERATOR:
            return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME+permissionConfig.getName(), actions);

		case TRANSACTION:
			return new TransactionPermission();
			
		case ALL:
			return new AllPermissions();
			
		default:
			throw new IllegalArgumentException(permissionConfig.getType().toString());
		}
	}
	
	public static boolean addressMatches(final String address, final String pattern) {
		return AddressUtil.matchInterface(address, pattern);
	}
	
	public static String getCredentialsFullName(Credentials credentials) {
		if(credentials == null) {
			return null;
		}
		return credentials.getPrincipal() + '@' + credentials.getEndpoint();
	}
	
	private SecurityUtil() {}
}
