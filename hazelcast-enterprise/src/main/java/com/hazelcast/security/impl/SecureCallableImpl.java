package com.hazelcast.security.impl;

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
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;
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
import com.hazelcast.security.SecureCallable;
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
import com.hazelcast.security.permission.SqlPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
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
import java.util.UUID;
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
        public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(task);
        }

        @Override
        public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                        @Nonnull TransactionalTask<T> task) throws TransactionException {
            checkPermission(new TransactionPermission());
            return instance.executeTransaction(options, task);
        }

        @Override
        public TransactionContext newTransactionContext() {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext();
        }

        @Override
        public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
            checkPermission(new TransactionPermission());
            return instance.newTransactionContext(options);
        }

        @Nonnull
        @Override
        public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public ConcurrentMap<String, Object> getUserContext() {
            return instance.getUserContext();
        }

        @Nonnull
        @Override
        public <E> IQueue<E> getQueue(@Nonnull String name) {
            checkPermission(new QueuePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IQueueInvocationHandler(instance.getQueue(name)));
        }

        @Nonnull
        @Override
        public <E> ITopic<E> getTopic(@Nonnull String name) {
            checkPermission(new TopicPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ITopicInvocationHandler(instance.getTopic(name)));
        }

        @Nonnull
        @Override
        public <E> ISet<E> getSet(@Nonnull String name) {
            checkPermission(new SetPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ISetInvocationHandler(instance.getSet(name)));
        }

        @Nonnull
        @Override
        public <E> IList<E> getList(@Nonnull String name) {
            checkPermission(new ListPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IListInvocationHandler(instance.getList(name)));
        }

        @Nonnull
        @Override
        public <K, V> IMap<K, V> getMap(@Nonnull String name) {
            checkPermission(new MapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IMapInvocationHandler(instance.getMap(name)));
        }

        @Nonnull
        @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
            checkPermission(new ReplicatedMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ReplicatedMapInvocationHandler(instance.getReplicatedMap(name)));
        }

        @Nonnull
        @Override
        public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
            checkPermission(new MultiMapPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new MultiMapInvocationHandler(instance.getMultiMap(name)));
        }

        @Nonnull
        @Override
        public IExecutorService getExecutorService(@Nonnull String name) {
            checkPermission(new ExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ExecutorServiceInvocationHandler(instance.getExecutorService(name)));
        }

        @Nonnull
        @Override
        public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
            checkPermission(new DurableExecutorServicePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new DurableExecutorServiceInvocationHandler(instance.getDurableExecutorService(name)));
        }

        @Nonnull
        @Override
        public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
            checkPermission(new FlakeIdGeneratorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new FlakeIdGeneratorInvocationHandler(instance.getFlakeIdGenerator(name)));
        }

        @Nonnull
        @Override
        public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
            checkPermission(new CardinalityEstimatorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new CardinalityEstimatorHandler(instance.getCardinalityEstimator(name)));
        }

        @Nonnull
        @Override
        public PNCounter getPNCounter(@Nonnull String name) {
            checkPermission(new PNCounterPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new PNCounterHandler(instance.getPNCounter(name)));
        }

        @Nonnull
        @Override
        public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
            checkPermission(new ScheduledExecutorPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ScheduledExecutorHandler(instance.getScheduledExecutorService(name)));
        }

        @Override
        public ICacheManager getCacheManager() {
            return new CacheManagerDelegate(instance.getCacheManager());
        }

        @Nonnull
        @Override
        public Cluster getCluster() {
            return instance.getCluster();
        }

        @Nonnull
        @Override
        public Endpoint getLocalEndpoint() {
            return instance.getLocalEndpoint();
        }

        @Nonnull
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
        public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Config getConfig() {
            return instance.getConfig();
        }

        @Nonnull
        @Override
        public PartitionService getPartitionService() {
            return instance.getPartitionService();
        }

        @Nonnull
        @Override
        public ClientService getClientService() {
            return instance.getClientService();
        }

        @Nonnull
        @Override
        public LoggingService getLoggingService() {
            return instance.getLoggingService();
        }

        @Nonnull
        @Override
        public LifecycleService getLifecycleService() {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public SplitBrainProtectionService getSplitBrainProtectionService() {
            return instance.getSplitBrainProtectionService();
        }

        @Nonnull
        @Override
        public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
            return instance.getRingbuffer(name);
        }

        @Nonnull
        @Override
        public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
            return instance.getReliableTopic(name);
        }

        @Nonnull
        @Override
        public HazelcastXAResource getXAResource() {
            return instance.getXAResource();
        }

        @Nonnull
        @Override
        public CPSubsystem getCPSubsystem() {
            return new CPSubsystemDelegate(instance.getCPSubsystem());
        }

        @Nonnull
        @Override
        public SqlService getSql() {
            return new SqlServiceDelegate(instance.getSql());
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

        @Nonnull
        @Override
        public IAtomicLong getAtomicLong(@Nonnull String name) {
            checkPermission(new AtomicLongPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicLongInvocationHandler(cpSubsystem.getAtomicLong(name)));
        }

        @Nonnull
        @Override
        public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
            checkPermission(new AtomicReferencePermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new IAtomicReferenceInvocationHandler(cpSubsystem.getAtomicReference(name)));
        }

        @Nonnull
        @Override
        public ICountDownLatch getCountDownLatch(@Nonnull String name) {
            checkPermission(new CountDownLatchPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new ICountDownLatchInvocationHandler(cpSubsystem.getCountDownLatch(name)));
        }

        @Nonnull
        @Override
        public FencedLock getLock(@Nonnull String name) {
            checkPermission(new LockPermission(name, ActionConstants.ACTION_CREATE));
            return getProxy(new LockInvocationHandler(cpSubsystem.getLock(name)));
        }

        @Nonnull
        @Override
        public ISemaphore getSemaphore(@Nonnull String name) {
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

    private class SqlServiceDelegate implements SqlService {
        private final SqlService sqlService;

        SqlServiceDelegate(SqlService sqlService) {
            this.sqlService = sqlService;
        }

        @Nonnull
        @Override
        public SqlResult query(@Nonnull String sql, Object... params) {
            checkPermission(new SqlPermission());
            return sqlService.query(sql, params);
        }

        @Nonnull
        @Override
        public SqlResult query(@Nonnull SqlQuery query) {
            checkPermission(new SqlPermission());
            return sqlService.query(query);
        }
    }
}
