package com.hazelcast.security.permissions;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.security.SecureCallableImpl;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class PermissionMappingTest extends HazelcastTestSupport {

    // Insecurely accessed services
    private static final Set<Class> INSECURE_SERVICES = new HashSet<Class>();

    static {
        INSECURE_SERVICES.add(com.hazelcast.cache.impl.EnterpriseCacheService.class);
        INSECURE_SERVICES.add(com.hazelcast.serviceprovider.TestRemoteService.class);
        INSECURE_SERVICES.add(com.hazelcast.ringbuffer.impl.RingbufferService.class);
        INSECURE_SERVICES.add(com.hazelcast.topic.impl.reliable.ReliableTopicService.class);
        INSECURE_SERVICES.add(com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorService.class);
        INSECURE_SERVICES.add(com.hazelcast.transaction.impl.xa.XAService.class);
        INSECURE_SERVICES.add(com.hazelcast.cp.internal.datastructures.lock.RaftLockService.class);
        INSECURE_SERVICES.add(com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService.class);
    }

    // Common methods that don't require security
    private static final Set<String> UNIVERSAL_METHOD_SKIP_LIST = new HashSet<String>();

    static {
        UNIVERSAL_METHOD_SKIP_LIST.addAll(
                asList("toString", "getName", "getServiceName", "getService", "getPartitionId", "invalidate",
                        "getOperationService", "getPartitionKey", "getNodeEngine"));
    }

    // Common methods that don't require security
    private static final Set<Class> INHERITED_METHOD_SKIP_LIST = new HashSet<Class>();

    static {
        INHERITED_METHOD_SKIP_LIST.add(Object.class);
        INHERITED_METHOD_SKIP_LIST.add(Iterable.class);
    }

    /**
     * Mappings of services to structure name as seen in the respective
     * {@link SecureCallableImpl.SecureInvocationHandler#getStructureName()}
     * implementation.
     */
    private static final Map<Class, String> SERVICE_TO_PERMSTRUCT_MAPPING = new HashMap<Class, String>();

    static {
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongService.class,
                "atomicLong");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(AtomicRefService.class,
                "atomicReference");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cardinality.impl.CardinalityEstimatorService.class,
                "cardinalityEstimator");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.class,
                "durableExecutorService");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.topic.impl.TopicService.class,
                "topic");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.map.impl.MapService.class,
                "map");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.executor.impl.DistributedExecutorService.class,
                "executorService");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService.class,
                "countDownLatch");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.collection.impl.queue.QueueService.class,
                "queue");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.collection.impl.list.ListService.class,
                "list");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorService.class,
                "idGenerator");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.collection.impl.set.SetService.class,
                "set");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.replicatedmap.impl.ReplicatedMapService.class,
                "replicatedMap");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cp.internal.datastructures.unsafe.lock.LockServiceImpl.class,
                "lock");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService.class,
                "semaphore");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.multimap.impl.MultiMapService.class,
                "multiMap");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.class,
                "scheduledExecutor");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService.class,
                "flakeIdGenerator");
        SERVICE_TO_PERMSTRUCT_MAPPING.put(com.hazelcast.internal.crdt.pncounter.PNCounterService.class,
                "PNCounter");
    }

    private static final Map<Class, String[]> PER_SERVICE_SKIP_LIST = new HashMap<Class, String[]>();

    static {
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.multimap.impl.MultiMapService.class, new String[]{"initialize"});
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.collection.impl.list.ListService.class, new String[]{
                "initialize", "removeIf", "stream", "parallelStream", "replaceAll", "sort",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.collection.impl.queue.QueueService.class, new String[]{
                "initialize", "removeIf", "stream", "parallelStream",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.collection.impl.set.SetService.class, new String[]{
                "initialize", "removeIf", "stream", "parallelStream",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.class,
                new String[]{
                        "scheduleOnMemberAtFixedRate", "scheduleAtFixedRate", "scheduleOnKeyOwner",
                        "scheduleOnAllMembers", "scheduleOnKeyOwnerAtFixedRate", "schedule", "scheduleOnMember",
                        "scheduleOnAllMembersAtFixedRate", "getScheduledFuture", "scheduleOnMembers",
                        "scheduleOnMembersAtFixedRate", "getAllScheduledFutures", "shutdown", "destroy",
                });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.class,
                new String[]{
                        "retrieveResult", "submit", "isTerminated", "invokeAll", "executeOnKeyOwner",
                        "retrieveAndDisposeResult", "execute", "disposeResult", "awaitTermination",
                        "shutdownNow", "invokeAny", "submitToKeyOwner", "shutdown", "isShutdown", "destroy",
                });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.executor.impl.DistributedExecutorService.class, new String[]{
                "getLocalExecutorStats", "executeOnMembers", "executeOnMember", "submit", "isTerminated",
                "invokeAll", "submitToAllMembers", "executeOnKeyOwner", "submitToMember",
                "submitToMembers", "execute", "executeOnAllMembers", "awaitTermination", "shutdownNow",
                "invokeAny", "submitToKeyOwner", "shutdown", "isShutdown", "destroy",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.internal.crdt.pncounter.PNCounterService.class, new String[]{
                "getCurrentTargetReplicaAddress", "setOperationTryCount", "reset",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.replicatedmap.impl.ReplicatedMapService.class, new String[]{
                "getOrDefault", "computeIfAbsent", "compute", "computeIfPresent",
                "initialize", "putIfAbsent", "merge", "replace", "replaceAll",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.topic.impl.TopicService.class, new String[]{"initialize"});
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.map.impl.MapService.class, new String[]{
                "getOrDefault", "compute", "merge", "computeIfAbsent", "setOperationProvider",
                "getOperationProvider", "computeIfPresent", "getPartitionStrategy", "waitUntilLoaded",
                "initialize", "getQueryCache", "getTotalBackupCount", "subscribeToEventJournal",
                "replace", "replaceAll",
        });
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService.class,
                new String[] {"getGroupId"});
        PER_SERVICE_SKIP_LIST.put(com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService.class,
                new String[] {"getGroupId"});
        PER_SERVICE_SKIP_LIST.put(AtomicRefService.class,
                new String[] {"getGroupId"});
    }

    @Test
    public void testForMissingPermissionMappingProperties() {
        Map<String, Set<String>> permissionMappings = readPermissionMappings();

        HazelcastInstance instance = createHazelcastInstance();
        ServiceManagerImpl serviceManager = (ServiceManagerImpl)
                getNodeEngineImpl(instance).getServiceManager();

        Map<Class, Set<String>> missingPerms = new HashMap<Class, Set<String>>();

        List<Object> serviceList = new ArrayList<>();
        serviceList.addAll(serviceManager.getServices(RemoteService.class));
        serviceList.addAll(serviceManager.getServices(RaftRemoteService.class));

        for (Object service : serviceList) {
            if (INSECURE_SERVICES.contains(service.getClass())) {
                continue;
            }

            if (!SERVICE_TO_PERMSTRUCT_MAPPING.containsKey(service.getClass())) {
                fail("Remote service '" + service.getClass() + "' not listed in permission name mappings.");
            }

            DistributedObject dObj;
            if (service instanceof RemoteService) {
                dObj = ((RemoteService) service).createDistributedObject("TestingPermissionMappings");
            } else {
                dObj = ((RaftRemoteService) service).createProxy("TestingPermissionMappings");
            }
            Method[] methods = dObj.getClass().getMethods();
            Set<String> filteredMethods = new HashSet<String>();

            outer:
            for (Method m : methods) {
                boolean isPublicAPI = Modifier.isPublic(m.getModifiers());
                boolean shouldSkip = UNIVERSAL_METHOD_SKIP_LIST.contains(m.getName())
                        || (PER_SERVICE_SKIP_LIST.get(service.getClass()) != null
                        && asList(PER_SERVICE_SKIP_LIST.get(service.getClass())).contains(m.getName()));

                // Check if methods is inherited from a skipped super-class
                for (Class c : INHERITED_METHOD_SKIP_LIST) {
                    for (Method superMethod : c.getMethods()) {
                        if (superMethod.getName().equals(m.getName())) {
                            continue outer;
                        }
                    }
                }

                if (isPublicAPI && !shouldSkip) {
                    filteredMethods.add(m.getName());
                }
            }

            String permName = SERVICE_TO_PERMSTRUCT_MAPPING.get(service.getClass());
            Set<String> mappings = permissionMappings.get(permName);
            if (!filteredMethods.isEmpty()) {
                if (mappings == null) {
                    missingPerms.put(service.getClass(), filteredMethods);
                } else {
                    filteredMethods.removeAll(mappings);
                    if (!filteredMethods.isEmpty()) {
                        missingPerms.put(service.getClass(), filteredMethods);
                    }
                }
            }
        }

        if (!missingPerms.isEmpty()) {
            fail(generateMissingPermsReport(missingPerms));
        }
    }

    @Test
    public void testForObsoletePermissionMappingProperties() {
        Map<String, Set<String>> permissionMappings = readPermissionMappings();

        HazelcastInstance instance = createHazelcastInstance();
        ServiceManagerImpl serviceManager = (ServiceManagerImpl)
                getNodeEngineImpl(instance).getServiceManager();

        for (Map.Entry<String, Set<String>> permEntry : permissionMappings.entrySet()) {
            String serviceAlias = permEntry.getKey();
            Class serviceClass = null;

            // Could improve lookup with a bidirectional map, but its rather small collection
            for (Map.Entry<Class, String> service : SERVICE_TO_PERMSTRUCT_MAPPING.entrySet()) {
                if (serviceAlias.equals(service.getValue())) {
                    serviceClass = service.getKey();
                    break;
                }
            }

            assertNotNull("Service alias " + serviceAlias + " not found in services. Obsolete service mapping?", serviceClass);
            List<Object> services = serviceManager.getServices(serviceClass);
            assertEquals("Found more than one service with the given class " + serviceClass
                    + ": " + Arrays.toString(services.toArray()), 1, services.size());

            Object service = services.get(0);
            DistributedObject proxy;
            if (service instanceof RemoteService) {
                proxy = ((RemoteService) service).createDistributedObject("Test");
            } else {
                proxy = ((RaftRemoteService) service).createProxy("Test");
            }

            for (String mappedMethod : permEntry.getValue()) {
                Method match = null;
                Method[] api = proxy.getClass().getMethods();

                for (Method apiMethod : api) {
                    if (apiMethod.getName().equals(mappedMethod)) {
                        match = apiMethod;
                        break;
                    }
                }

                assertNotNull("Obsolete permission mapping " + serviceAlias + ":" + mappedMethod, match);
                assertTrue("Obsolete permission mapping for private API " + serviceAlias + ":"
                        + mappedMethod, Modifier.isPublic(match.getModifiers()));

                String[] serviceSkipList = PER_SERVICE_SKIP_LIST.get(serviceClass);
                if (serviceSkipList != null) {
                    assertNotContains(asList(serviceSkipList), mappedMethod);
                }
            }
        }
    }


    private String generateMissingPermsReport(Map<Class, Set<String>> map) {
        StringBuilder builder = new StringBuilder();
        builder.append("Service operations missing from 'permission-mapping.properties':\n\n");

        for (Map.Entry<Class, Set<String>> entry : map.entrySet()) {
            builder.append(entry.getKey() + "\n");
            for (String missing : entry.getValue()) {
                builder.append("\t" + missing + "\n");
            }
        }

        builder.append("\n");
        builder.append(
                "Note: there are cases where direct support for a client operation is not provided, eg. a client can't call "
                        + "an API (not available or not implemented), however, clients can still execute code within an executor,"
                        + "therefore **all** relevant calls must have a permission type.");
        builder.append("\n");

        return builder.toString();
    }

    private Map<String, Set<String>> readPermissionMappings() {
        Map<String, Set<String>> permissionMappings = new HashMap<String, Set<String>>();

        final AssertNoDupsProperties properties = new AssertNoDupsProperties();
        final ClassLoader cl = SecureCallableImpl.class.getClassLoader();
        final InputStream stream = cl.getResourceAsStream("permission-mapping.properties");
        try {
            properties.load(stream);
        } catch (IOException e) {
            ExceptionUtil.rethrow(e);
        } finally {
            closeResource(stream);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            final String key = (String) entry.getKey();
            final int dotIndex = key.indexOf('.');
            if (dotIndex == -1) {
                continue;
            }
            final String structure = key.substring(0, dotIndex).trim();
            final String method = key.substring(dotIndex + 1).trim();
            Set<String> methods = permissionMappings.computeIfAbsent(structure, k -> new HashSet<String>());

            methods.add(method);
        }

        return permissionMappings;
    }

    class AssertNoDupsProperties extends Properties {
        @Override
        public synchronized Object put(Object key, Object value) {
            assertFalse("Found duplicate mapping for " + key, this.keySet().contains(key));
            return super.put(key, value);
        }
    }
}
