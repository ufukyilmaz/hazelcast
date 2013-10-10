package com.hazelcast.security;

import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.proxy.LockProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.permission.*;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.util.ExceptionUtil;

import javax.security.auth.Subject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.Permission;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public final class SecureCallableImpl<V> implements SecureCallable<V>, DataSerializable {
	
	private transient Node node;
	private Subject subject;
	private Callable<V> callable;
	
	public SecureCallableImpl() {
		super();
	}
	
	public SecureCallableImpl(Subject subject, Callable<V> callable) {
		super();
		this.subject = subject;
		this.callable = callable;
	}

	public V call() throws Exception {
		return callable.call();
	}
	
	public Subject getSubject() {
		return subject;
	}

	public String toString() {
		return "SecureCallable [subject=" + subject + ", callable=" + callable
				+ "]";
	}

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(callable);
        boolean hasSubject = subject != null;
        out.writeBoolean(hasSubject);
        if(hasSubject) {
            final Set<Principal> principals = subject.getPrincipals();
            out.writeInt(principals.size());
            for (Principal principal : principals) {
                out.writeObject(principal);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        callable = in.readObject();
        boolean hasSubject = in.readBoolean();
        if(hasSubject) {
            subject = new Subject();
            int size = in.readInt();
            final Set<Principal> principals = subject.getPrincipals();
            for (int i = 0; i < size; i++) {
                Principal principal = in.readObject();
                principals.add(principal);
            }
        }
    }

	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		if(callable instanceof HazelcastInstanceAware) {
			((HazelcastInstanceAware) callable).setHazelcastInstance(new HazelcastInstanceDelegate(hazelcastInstance));
		}
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	private class HazelcastInstanceDelegate implements HazelcastInstance {
		private final HazelcastInstance instance;
		HazelcastInstanceDelegate(HazelcastInstance instance) {
			super();
			this.instance = instance;
		}
        public ILock getLock(String key) {
            checkPermission(new LockPermission(key, ActionConstants.ACTION_CREATE));
            return getProxy(new ILockInvocationHandler(instance.getLock(key)));
        }
        public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(task);
        }
        public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(options, task);
        }
        public TransactionContext newTransactionContext() {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext();
        }
        public TransactionContext newTransactionContext(TransactionOptions options) {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext(options);
        }
        public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
            throw new UnsupportedOperationException();
        }
        public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
            throw new UnsupportedOperationException();
        }
        public ConcurrentMap<String, Object> getUserContext() {
            return instance.getUserContext();
        }
        public <E> IQueue<E> getQueue(final String name) {
            checkPermission(new QueuePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IQueueInvocationHandler(instance.getQueue(name)));
		}
		public <E> ITopic<E> getTopic(final String name) {
            checkPermission(new TopicPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ITopicInvocationHandler(instance.getTopic(name)));
		}
		public <E> ISet<E> getSet(final String name) {
            checkPermission(new SetPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISetInvocationHandler(instance.getSet(name)));
		}
		public <E> IList<E> getList(final String name) {
            checkPermission(new ListPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IListInvocationHandler(instance.getList(name)));
		}
		public <K, V> IMap<K, V> getMap(final String name) {
            checkPermission(new MapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IMapInvocationHandler(instance.getMap(name)));
		}
		public <K, V> MultiMap<K, V> getMultiMap(final String name) {
            checkPermission(new MultiMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new MultiMapInvocationHandler(instance.getMultiMap(name)));
		}
		public ILock getLock(final Object key) {
            final String name = LockProxy.convertToStringKey(key, node.getSerializationService());
            checkPermission(new LockPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ILockInvocationHandler(instance.getLock(name)));
		}
		public IExecutorService getExecutorService(final String name) {
            checkPermission(new ExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ExecutorServiceInvocationHandler(instance.getExecutorService(name)));
		}
		public IdGenerator getIdGenerator(final String name) {
            checkPermission(new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME+name, ActionConstants.ACTION_CREATE));
            return getProxy(new IdGeneratorInvocationHandler(instance.getIdGenerator(name)));
		}
		public IAtomicLong getAtomicLong(final String name) {
            checkPermission(new AtomicLongPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicLongInvocationHandler(instance.getAtomicLong(name)));
		}
		public ICountDownLatch getCountDownLatch(final String name) {
            checkPermission(new CountDownLatchPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ICountDownLatchInvocationHandler(instance.getCountDownLatch(name)));
		}
		public ISemaphore getSemaphore(final String name) {
            checkPermission(new SemaphorePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISemaphoreInvocationHandler(instance.getSemaphore(name)));
		}
		public Cluster getCluster() {
			return instance.getCluster();
		}
		public String getName() {
			return instance.getName();
		}
		public void shutdown() {
			throw new UnsupportedOperationException();
		}
		public void restart() {
			throw new UnsupportedOperationException();
		}
		public Collection<DistributedObject> getDistributedObjects() {
			return instance.getDistributedObjects();
		}
        public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
            throw new UnsupportedOperationException();
        }
        public boolean removeDistributedObjectListener(String registrationId) {
            throw new UnsupportedOperationException();
        }
		public Config getConfig() {
			return instance.getConfig();
		}
        public PartitionService getPartitionService() {
            return instance.getPartitionService();
        }

        public ClientService getClientService() {
            return instance.getClientService();
        }
        public LoggingService getLoggingService() {
			return instance.getLoggingService();
		}
		public LifecycleService getLifecycleService() {
			throw new UnsupportedOperationException();
		}
	}

    private <T> T getProxy(SecureInvocationHandler handler) {
        final DistributedObject distributedObject = handler.getDistributedObject();
        final Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(), getAllInterfaces(distributedObject), handler);
        return (T) proxy;
    }

    public void checkPermission(Permission permission){
        node.securityContext.checkPermission(subject, permission);
    }

	
	public static Class[] getAllInterfaces(Object instance) {
		Class clazz = instance.getClass();
		Set<Class> all = new HashSet<Class>();
		while (clazz != null) {
			final Class[] interfaces = clazz.getInterfaces();
			for (int i = 0; i < interfaces.length; i++) {
				all.add(interfaces[i]);
			}
			clazz = clazz.getSuperclass();
		}
		return (Class[]) all.toArray(new Class[all.size()]);
	}

    static final Map<String, Map<String,String>> structureMethodMap = new HashMap<String, Map<String, String>>();
    static {
        final Properties properties = new Properties();
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final InputStream stream = cl.getResourceAsStream("META-INF/services/com.hazelcast.permission-mapping");
        try {
            properties.load(stream);
        } catch (IOException e) {
            ExceptionUtil.rethrow(e);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            final String key = (String)entry.getKey();
            final String action = (String)entry.getValue();
            final int dotIndex = key.indexOf('.');
            if (dotIndex == -1){
                continue;
            }
            final String structure = key.substring(0, dotIndex);
            final String method = key.substring(dotIndex+1);
            Map<String, String> methodMap = structureMethodMap.get(structure);
            if (methodMap == null){
                methodMap = new HashMap<String, String>();
                structureMethodMap.put(structure, methodMap);
            }
            methodMap.put(method,action);
        }
    }

    public static void main(String[] args) {
        final String s = structureMethodMap.get("lock").get("isLocked");
        System.err.println("s: " + s);
    }

    private abstract class SecureInvocationHandler implements InvocationHandler {

		protected final DistributedObject distributedObject;
        private final Map<String, String> methodMap;

		SecureInvocationHandler(DistributedObject distributedObject) {
			this.distributedObject = distributedObject;
            methodMap = structureMethodMap.get(getStructureName());
		}

        public DistributedObject getDistributedObject() {
            return distributedObject;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final Permission permission = getPermission(method, args);
            if (permission != null){
                checkPermission(permission);
            }
            return method.invoke(distributedObject, args);
        }

        public abstract Permission getPermission(Method method, Object[] args);
        public abstract String getStructureName();
        public String getAction(String methodName){
            return methodMap.get(methodName);
        }

	}

    private class ILockInvocationHandler extends SecureInvocationHandler {

        ILockInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new LockPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "lock";
        }
    }

    private class IQueueInvocationHandler extends SecureInvocationHandler {

        IQueueInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new QueuePermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "queue";
        }
    }

    private class ITopicInvocationHandler extends SecureInvocationHandler {

        ITopicInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new TopicPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "topic";
        }
    }

    private class ISetInvocationHandler extends SecureInvocationHandler {

        ISetInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new SetPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "set";
        }
    }

    private class IListInvocationHandler extends SecureInvocationHandler {

        IListInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new ListPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "list";
        }
    }

    private class IMapInvocationHandler extends SecureInvocationHandler {

        IMapInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("executeOnKey") || method.getName().equals("executeOnEntries")){
                return new MapPermission(distributedObject.getName(), ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
            }
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new MapPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "map";
        }
    }

    private class MultiMapInvocationHandler extends SecureInvocationHandler {

        MultiMapInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new MultiMapPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "multiMap";
        }
    }

    private class ExecutorServiceInvocationHandler extends SecureInvocationHandler {

        ExecutorServiceInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("destroy")){
                return new ExecutorServicePermission(distributedObject.getName(), ActionConstants.ACTION_DESTROY);
            }
            return null;
        }

        public String getStructureName() {
            return "executorService";
        }
    }

    private class IAtomicLongInvocationHandler extends SecureInvocationHandler {

        IAtomicLongInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new AtomicLongPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "atomicLong";
        }
    }

    private class ICountDownLatchInvocationHandler extends SecureInvocationHandler {

        ICountDownLatchInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new CountDownLatchPermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "countDownLatch";
        }
    }

    private class ISemaphoreInvocationHandler extends SecureInvocationHandler {

        ISemaphoreInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null){
                return null;
            }
            return new SemaphorePermission(distributedObject.getName(), action);
        }

        public String getStructureName() {
            return "semaphore";
        }
    }

    private class IdGeneratorInvocationHandler extends SecureInvocationHandler {

        IdGeneratorInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("destroy")){
                return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME+distributedObject.getName(), ActionConstants.ACTION_DESTROY);
            }
            return null;
        }

        public String getStructureName() {
            return "idGenerator";
        }
    }
}