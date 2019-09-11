package com.hazelcast.security;

import com.hazelcast.cache.ICache;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Endpoint;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.security.impl.SecurityDataSerializerHook;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.CardinalityEstimatorPermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.security.permission.PNCounterPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.AccessControlException;
import java.security.Permission;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"})
@SuppressFBWarnings(value = "SE_NO_SERIALVERSIONID",
        justification = "we implement IdentifiedDataSerializable, so Serializable is not used")
public final class SecureCallableImpl<V> implements SecureCallable<V>, IdentifiedDataSerializable {

    private Map<String, Map<String, String>> serviceToMethod;
    private Subject subject;
    private Object taskObject;
    private boolean blockUnmappedActions;

    private transient Node node;

    public SecureCallableImpl() {
    }

    public SecureCallableImpl(Subject subject, Callable<V> taskObject, Map<String, Map<String, String>> serviceToMethod) {
        this.subject = subject;
        this.taskObject = taskObject;
        this.serviceToMethod = serviceToMethod;
    }

    public SecureCallableImpl(Subject subject, Runnable runnable, Map<String, Map<String, String>> serviceToMethod) {
        this.subject = subject;
        this.taskObject = runnable;
        this.serviceToMethod = serviceToMethod;
    }

    @Override
    public V call() throws Exception {
        if (taskObject instanceof Runnable) {
            ((Runnable) taskObject).run();
            return null;
        }
        return ((Callable<V>) taskObject).call();
    }

    @Override
    public String toString() {
        return "SecureCallable [subject=" + subject + ", taskObject=" + taskObject + "]";
    }

    @Override
    public int getFactoryId() {
        return SecurityDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SecurityDataSerializerHook.SECURE_CALLABLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(taskObject);
        boolean hasSubject = subject != null;
        out.writeBoolean(hasSubject);
        if (hasSubject) {
            Set<Principal> principals = subject.getPrincipals();
            out.writeInt(principals.size());
            for (Principal principal : principals) {
                out.writeObject(principal);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        taskObject = in.readObject();
        boolean hasSubject = in.readBoolean();
        if (hasSubject) {
            subject = new Subject();
            int size = in.readInt();
            Set<Principal> principals = subject.getPrincipals();
            for (int i = 0; i < size; i++) {
                Principal principal = in.readObject();
                principals.add(principal);
            }
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (taskObject instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) taskObject).setHazelcastInstance(new HazelcastInstanceDelegate(hazelcastInstance));
        }
    }

    @Override
    public void setNode(Node node) {
        this.node = node;
        this.blockUnmappedActions = node.getConfig().getSecurityConfig().getClientBlockUnmappedActions();
        this.serviceToMethod = ((SecurityContextImpl) node.getNodeExtension().getSecurityContext()).getServiceToMethod();
    }

    private <T> T getProxy(SecureInvocationHandler handler) {
        DistributedObject distributedObject = handler.getDistributedObject();
        Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(), getAllInterfaces(distributedObject), handler);
        return (T) proxy;
    }

    private void checkPermission(Permission permission) {
        node.securityContext.checkPermission(subject, permission);
    }

    private static Class[] getAllInterfaces(Object instance) {
        Class clazz = instance.getClass();
        Set<Class> all = new HashSet<Class>();
        while (clazz != null) {
            Collections.addAll(all, clazz.getInterfaces());
            clazz = clazz.getSuperclass();
        }
        return all.toArray(new Class[0]);
    }

    private class HazelcastInstanceDelegate implements HazelcastInstance {

        private final HazelcastInstance instance;

        HazelcastInstanceDelegate(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public ILock getLock(String key) {
            checkPermission(new LockPermission(key, ActionConstants.ACTION_CREATE));
            return getProxy(new LockInvocationHandler(instance.getLock(key)));
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
        public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConcurrentMap<String, Object> getUserContext() {
            return instance.getUserContext();
        }

        @Override
        public <E> IQueue<E> getQueue(String name) {
            checkPermission(new QueuePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IQueueInvocationHandler(instance.getQueue(name)));
        }

        @Override
        public <E> ITopic<E> getTopic(String name) {
            checkPermission(new TopicPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ITopicInvocationHandler(instance.getTopic(name)));
        }

        @Override
        public <E> ISet<E> getSet(String name) {
            checkPermission(new SetPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISetInvocationHandler(instance.getSet(name)));
        }

        @Override
        public <E> IList<E> getList(String name) {
            checkPermission(new ListPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IListInvocationHandler(instance.getList(name)));
        }

        @Override
        public <K, V> IMap<K, V> getMap(String name) {
            checkPermission(new MapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IMapInvocationHandler(instance.getMap(name)));
        }

        @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
            checkPermission(new ReplicatedMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ReplicatedMapInvocationHandler(instance.getReplicatedMap(name)));
        }

        @Override
        public <K, V> MultiMap<K, V> getMultiMap(String name) {
            checkPermission(new MultiMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new MultiMapInvocationHandler(instance.getMultiMap(name)));
        }

        @Override
        public IExecutorService getExecutorService(String name) {
            checkPermission(new ExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ExecutorServiceInvocationHandler(instance.getExecutorService(name)));
        }

        @Override
        public DurableExecutorService getDurableExecutorService(String name) {
            checkPermission(new DurableExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new DurableExecutorServiceInvocationHandler(instance.getDurableExecutorService(name)));
        }

        @Override
        public IdGenerator getIdGenerator(String name) {
            checkPermission(new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + name, ActionConstants.ACTION_CREATE));
            return getProxy(new IdGeneratorInvocationHandler(instance.getIdGenerator(name)));
        }

        @Override
        public FlakeIdGenerator getFlakeIdGenerator(String name) {
            checkPermission(new FlakeIdGeneratorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new FlakeIdGeneratorInvocationHandler(instance.getFlakeIdGenerator(name)));
        }

        @Override
        public IAtomicLong getAtomicLong(String name) {
            checkPermission(new AtomicLongPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicLongInvocationHandler(instance.getAtomicLong(name)));
        }

        @Override
        public <E> IAtomicReference<E> getAtomicReference(String name) {
            checkPermission(new AtomicReferencePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicReferenceInvocationHandler(instance.getAtomicReference(name)));
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator(String name) {
            checkPermission(new CardinalityEstimatorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new CardinalityEstimatorHandler(instance.getCardinalityEstimator(name)));
        }

        @Override
        public PNCounter getPNCounter(String name) {
            checkPermission(new PNCounterPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new PNCounterHandler(instance.getPNCounter(name)));
        }

        @Override
        public IScheduledExecutorService getScheduledExecutorService(String name) {
            checkPermission(new ScheduledExecutorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ScheduledExecutorHandler(instance.getScheduledExecutorService(name)));
        }

        @Override
        public ICacheManager getCacheManager() {
            return new CacheManagerDelegate(instance.getCacheManager());
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
        public SplitBrainProtectionService getSplitBrainProtectionService() {
            return instance.getSplitBrainProtectionService();
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

        @Override
        public CPSubsystem getCPSubsystem() {
            return new CPSubsystemDelegate(instance.getCPSubsystem());
        }
    }

    private class CacheManagerDelegate implements ICacheManager {

        private final ICacheManager cacheManager;

        CacheManagerDelegate(ICacheManager cacheManager) {
            this.cacheManager = cacheManager;
        }

        @Override
        public <K, V> ICache<K, V> getCache(String name) {
            checkPermission(new CachePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new CacheInvocationHandler(cacheManager.getCache(name)));
        }
    }

    private abstract class SecureInvocationHandler implements InvocationHandler {

        final DistributedObject distributedObject;

        private final Map<String, String> methodMap;

        SecureInvocationHandler(DistributedObject distributedObject) {
            this.distributedObject = distributedObject;
            methodMap = serviceToMethod.getOrDefault(getStructureName(), Collections.emptyMap());
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Permission permission = getPermission(method, args);
            if (permission != null) {
                checkPermission(permission);
            } else if (blockUnmappedActions) {
                throw new AccessControlException("Missing permission-mapping for `" + getStructureName()
                        + "." + method.getName() + "()`. Action blocked!");
            }
            return method.invoke(distributedObject, args);
        }

        DistributedObject getDistributedObject() {
            return distributedObject;
        }

        abstract Permission getPermission(Method method, Object[] args);

        abstract String getStructureName();

        String getAction(String methodName) {
            return methodMap.get(methodName);
        }
    }

    private class LockInvocationHandler extends SecureInvocationHandler {

        LockInvocationHandler(DistributedObject distributedObject) {
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

    private class DurableExecutorServiceInvocationHandler extends SecureInvocationHandler {

        DurableExecutorServiceInvocationHandler(DistributedObject distributedObject) {
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
            return "durableExecutorService";
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

    private class FlakeIdGeneratorInvocationHandler extends SecureInvocationHandler {

        FlakeIdGeneratorInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new FlakeIdGeneratorPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "flakeIdGenerator";
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

    private class CacheInvocationHandler extends SecureInvocationHandler {

        CacheInvocationHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            // TODO: should we use full name (with prefixes)?
            // we are using simple name (without any prefix), not full name
            return new CachePermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "cache";
        }
    }

    private class CardinalityEstimatorHandler extends SecureInvocationHandler {

        CardinalityEstimatorHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new CardinalityEstimatorPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "cardinalityEstimator";
        }
    }

    private class ScheduledExecutorHandler extends SecureInvocationHandler {

        ScheduledExecutorHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new ScheduledExecutorPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "scheduledExecutor";
        }
    }

    private class PNCounterHandler extends SecureInvocationHandler {

        PNCounterHandler(DistributedObject distributedObject) {
            super(distributedObject);
        }

        @Override
        public Permission getPermission(Method method, Object[] args) {
            String action = getAction(method.getName());
            if (action == null) {
                return null;
            }
            return new PNCounterPermission(distributedObject.getName(), action);
        }

        @Override
        public String getStructureName() {
            return "PNCounter";
        }
    }

    private class CPSubsystemDelegate implements CPSubsystem {
        private final CPSubsystem cpSubsystem;

        CPSubsystemDelegate(CPSubsystem cpSubsystem) {
            this.cpSubsystem = cpSubsystem;
        }

        @Override
        public IAtomicLong getAtomicLong(String name) {
            checkPermission(new AtomicLongPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicLongInvocationHandler(cpSubsystem.getAtomicLong(name)));
        }

        @Override
        public <E> IAtomicReference<E> getAtomicReference(String name) {
            checkPermission(new AtomicReferencePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicReferenceInvocationHandler(cpSubsystem.getAtomicReference(name)));
        }

        @Override
        public ICountDownLatch getCountDownLatch(String name) {
            checkPermission(new CountDownLatchPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ICountDownLatchInvocationHandler(cpSubsystem.getCountDownLatch(name)));
        }

        @Override
        public FencedLock getLock(String name) {
            checkPermission(new LockPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new LockInvocationHandler(cpSubsystem.getLock(name)));
        }

        @Override
        public ISemaphore getSemaphore(String name) {
            checkPermission(new SemaphorePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISemaphoreInvocationHandler(cpSubsystem.getSemaphore(name)));
        }

        @Override
        public CPMember getLocalCPMember() {
            return cpSubsystem.getLocalCPMember();
        }

        @Override
        public CPSubsystemManagementService getCPSubsystemManagementService() {
            throw new UnsupportedOperationException("CPSubsystemManagementService is not available!");
        }

        @Override
        public CPSessionManagementService getCPSessionManagementService() {
            throw new UnsupportedOperationException("CPSessionManagementService is not available!");
        }
    }
}
