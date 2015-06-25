package com.hazelcast.security;

import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.HazelcastXAResource;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public final class SecureCallableImpl<V> implements SecureCallable<V>, DataSerializable {

    private transient Node node;
    private Subject subject;
    private Callable<V> callable;

    public SecureCallableImpl() {
    }

    public SecureCallableImpl(Subject subject, Callable<V> callable) {
        this.subject = subject;
        this.callable = callable;
    }

    @Override
    public V call() throws Exception {
        return callable.call();
    }

    @Override
    public String toString() {
        return "SecureCallable [subject=" + subject + ", callable=" + callable + "]";
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(callable);
        boolean hasSubject = subject != null;
        out.writeBoolean(hasSubject);
        if (hasSubject) {
            final Set<Principal> principals = subject.getPrincipals();
            out.writeInt(principals.size());
            for (Principal principal : principals) {
                out.writeObject(principal);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callable = in.readObject();
        boolean hasSubject = in.readBoolean();
        if (hasSubject) {
            subject = new Subject();
            int size = in.readInt();
            final Set<Principal> principals = subject.getPrincipals();
            for (int i = 0; i < size; i++) {
                Principal principal = in.readObject();
                principals.add(principal);
            }
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (callable instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) callable).setHazelcastInstance(new HazelcastInstanceDelegate(hazelcastInstance));
        }
    }

    @Override
    public void setNode(Node node) {
        this.node = node;
    }

    private class HazelcastInstanceDelegate implements HazelcastInstance {
        private final HazelcastInstance instance;

        HazelcastInstanceDelegate(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public ILock getLock(String key) {
            checkPermission(new LockPermission(key, ActionConstants.ACTION_CREATE));
            return getProxy(new ILockInvocationHandler(instance.getLock(key)));
        }

        @Override
        public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(task);
        }

        @Override
        public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(options, task);
        }

        @Override
        public TransactionContext newTransactionContext() {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext();
        }

        @Override
        public TransactionContext newTransactionContext(TransactionOptions options) {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext(options);
        }

        @Override
        public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConcurrentMap<String, Object> getUserContext() {
            return instance.getUserContext();
        }

        @Override
        public <E> IQueue<E> getQueue(final String name) {
            checkPermission(new QueuePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IQueueInvocationHandler(instance.getQueue(name)));
        }

        @Override
        public <E> ITopic<E> getTopic(final String name) {
            checkPermission(new TopicPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ITopicInvocationHandler(instance.getTopic(name)));
        }

        @Override
        public <E> ISet<E> getSet(final String name) {
            checkPermission(new SetPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISetInvocationHandler(instance.getSet(name)));
        }

        @Override
        public <E> IList<E> getList(final String name) {
            checkPermission(new ListPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IListInvocationHandler(instance.getList(name)));
        }

        @Override
        public <K, V> IMap<K, V> getMap(final String name) {
            checkPermission(new MapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IMapInvocationHandler(instance.getMap(name)));
        }

        @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
            checkPermission(new ReplicatedMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ReplicatedMapInvocationHandler(instance.getReplicatedMap(name)));
        }

        @Override
        public <K, V> MultiMap<K, V> getMultiMap(final String name) {
            checkPermission(new MultiMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new MultiMapInvocationHandler(instance.getMultiMap(name)));
        }

        @Override
        public IExecutorService getExecutorService(final String name) {
            checkPermission(new ExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ExecutorServiceInvocationHandler(instance.getExecutorService(name)));
        }

        @Override
        public IdGenerator getIdGenerator(final String name) {
            checkPermission(new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + name, ActionConstants.ACTION_CREATE));
            return getProxy(new IdGeneratorInvocationHandler(instance.getIdGenerator(name)));
        }

        @Override
        public IAtomicLong getAtomicLong(final String name) {
            checkPermission(new AtomicLongPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicLongInvocationHandler(instance.getAtomicLong(name)));
        }

        @Override
        public <E> IAtomicReference<E> getAtomicReference(final String name) {
            checkPermission(new AtomicReferencePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicReferenceInvocationHandler(instance.getAtomicReference(name)));
        }

        @Override
        public ICountDownLatch getCountDownLatch(final String name) {
            checkPermission(new CountDownLatchPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ICountDownLatchInvocationHandler(instance.getCountDownLatch(name)));
        }

        @Override
        public ISemaphore getSemaphore(final String name) {
            checkPermission(new SemaphorePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISemaphoreInvocationHandler(instance.getSemaphore(name)));
        }

        @Override
        public JobTracker getJobTracker(String name) {
            return getProxy(new JobTrackerInvocationHandler(instance.getJobTracker(name)));
        }

        @Override
        public Cluster getCluster() {
            return instance.getCluster();
        }

        @Override
        public Endpoint getLocalEndpoint() {
            return instance.getLocalEndpoint();
        }

        @Override
        public String getName() {
            return instance.getName();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<DistributedObject> getDistributedObjects() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeDistributedObjectListener(String registrationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Config getConfig() {
            return instance.getConfig();
        }

        @Override
        public PartitionService getPartitionService() {
            return instance.getPartitionService();
        }

        @Override
        public ClientService getClientService() {
            return instance.getClientService();
        }

        @Override
        public LoggingService getLoggingService() {
            return instance.getLoggingService();
        }

        @Override
        public LifecycleService getLifecycleService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public QuorumService getQuorumService() {
            return instance.getQuorumService();
        }

        @Override
        public <E> Ringbuffer<E> getRingbuffer(String name) {
            return instance.getRingbuffer(name);
        }

        @Override
        public <E> ITopic<E> getReliableTopic(String name) {
            return instance.getReliableTopic(name);
        }

        @Override
        public HazelcastXAResource getXAResource() {
            return instance.getXAResource();
        }
    }

    private <T> T getProxy(SecureInvocationHandler handler) {
        final DistributedObject distributedObject = handler.getDistributedObject();
        final Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(), getAllInterfaces(distributedObject), handler);
        return (T) proxy;
    }

    public void checkPermission(Permission permission) {
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

    static final Map<String, Map<String, String>> structureMethodMap = new HashMap<String, Map<String, String>>();

    static {
        final Properties properties = new Properties();
        final ClassLoader cl = SecureCallableImpl.class.getClassLoader();
        final InputStream stream = cl.getResourceAsStream("permission-mapping.properties");
        try {
            properties.load(stream);
        } catch (IOException e) {
            ExceptionUtil.rethrow(e);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            final String key = (String) entry.getKey();
            final String action = (String) entry.getValue();
            final int dotIndex = key.indexOf('.');
            if (dotIndex == -1) {
                continue;
            }
            final String structure = key.substring(0, dotIndex);
            final String method = key.substring(dotIndex + 1);
            Map<String, String> methodMap = structureMethodMap.get(structure);
            if (methodMap == null) {
                methodMap = new HashMap<String, String>();
                structureMethodMap.put(structure, methodMap);
            }
            methodMap.put(method, action);
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

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final Permission permission = getPermission(method, args);
            if (permission != null) {
                checkPermission(permission);
            }
            return method.invoke(distributedObject, args);
        }

        public abstract Permission getPermission(Method method, Object[] args);

        public abstract String getStructureName();

        public String getAction(String methodName) {
            return methodMap.get(methodName);
        }
    }

    private class ILockInvocationHandler extends SecureInvocationHandler {

        ILockInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new LockPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "lock";
        }
    }

    private class IQueueInvocationHandler extends SecureInvocationHandler {

        IQueueInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new QueuePermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "queue";
        }
    }

    private class ITopicInvocationHandler extends SecureInvocationHandler {

        ITopicInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new TopicPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "topic";
        }
    }

    private class ISetInvocationHandler extends SecureInvocationHandler {

        ISetInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new SetPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "set";
        }
    }

    private class IListInvocationHandler extends SecureInvocationHandler {

        IListInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new ListPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "list";
        }
    }

    private class IMapInvocationHandler extends SecureInvocationHandler {

        IMapInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("executeOnKey") || method.getName().equals("executeOnEntries")) {
                return new MapPermission(distributedObject.getName(), ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
            }
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new MapPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "map";
        }
    }

    private class MultiMapInvocationHandler extends SecureInvocationHandler {

        MultiMapInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new MultiMapPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "multiMap";
        }
    }

    private class ExecutorServiceInvocationHandler extends SecureInvocationHandler {

        ExecutorServiceInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("destroy")) {
                return new ExecutorServicePermission(distributedObject.getName(), ActionConstants.ACTION_DESTROY);
            }
            return null;
        }

        @Override
        public String getStructureName() {
            return "executorService";
        }
    }

    private class IAtomicLongInvocationHandler extends SecureInvocationHandler {

        IAtomicLongInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new AtomicLongPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "atomicLong";
        }
    }

    private class IAtomicReferenceInvocationHandler extends SecureInvocationHandler {

        IAtomicReferenceInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new AtomicReferencePermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "atomicReference";
        }
    }

    private class ICountDownLatchInvocationHandler extends SecureInvocationHandler {

        ICountDownLatchInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new CountDownLatchPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "countDownLatch";
        }
    }

    private class ISemaphoreInvocationHandler extends SecureInvocationHandler {

        ISemaphoreInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new SemaphorePermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "semaphore";
        }
    }

    private class IdGeneratorInvocationHandler extends SecureInvocationHandler {

        IdGeneratorInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            if (method.getName().equals("destroy")) {
                return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + distributedObject.getName(),
                        ActionConstants.ACTION_DESTROY);
            }
            return null;
        }

        @Override
        public String getStructureName() {
            return "idGenerator";
        }
    }

    private class ReplicatedMapInvocationHandler extends SecureInvocationHandler {

        ReplicatedMapInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            return null;
        }

        @Override
        public String getStructureName() {
            return "replicatedMap";
        }
    }

    private class JobTrackerInvocationHandler extends SecureInvocationHandler {

        JobTrackerInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            return null;
        }

        @Override
        public String getStructureName() {
            return "jobTracker";
        }
    }
}
