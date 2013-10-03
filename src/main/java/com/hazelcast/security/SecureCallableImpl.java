package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.util.ExceptionUtil;

import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
		private final HazelcastInstance hazelcast;
		HazelcastInstanceDelegate(HazelcastInstance hazelcast) {
			super();
			this.hazelcast = hazelcast;
		}
        public ILock getLock(String key) {
            return null;
        }
        public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
            return null;
        }
        public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
            return null;
        }
        public TransactionContext newTransactionContext() {
            return null;
        }
        public TransactionContext newTransactionContext(TransactionOptions options) {
            return null;
        }
        public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
            return null;
        }
        public <T extends DistributedObject> T getDistributedObject(String serviceName, String name) {
            return null;
        }
        public ConcurrentMap<String, Object> getUserContext() {
            return null;
        }
        public <E> IQueue<E> getQueue(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IQueue<E>>() {
				public IQueue<E> run() throws Exception {
					return hazelcast.getQueue(name);
				}
			});
		}
		public <E> ITopic<E> getTopic(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<ITopic<E>>() {
				public ITopic<E> run() throws Exception {
					return hazelcast.getTopic(name);
				}
			});
		}
		public <E> ISet<E> getSet(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<ISet<E>>() {
				public ISet<E> run() throws Exception {
					return hazelcast.getSet(name);
				}
			});
		}
		public <E> IList<E> getList(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IList<E>>() {
				public IList<E> run() throws Exception {
					return hazelcast.getList(name);
				}
			});
		}
		public <K, V> IMap<K, V> getMap(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IMap<K, V>>() {
				public IMap<K, V> run() throws Exception {
					return hazelcast.getMap(name);
				}
			});
		}
		public <K, V> MultiMap<K, V> getMultiMap(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<MultiMap<K, V>>() {
				public MultiMap<K, V> run() throws Exception {
					return hazelcast.getMultiMap(name);
				}
			});
		}
		public ILock getLock(final Object key) {
			return getInstanceProxy(new PrivilegedExceptionAction<ILock>() {
				public ILock run() throws Exception {
					return hazelcast.getLock(key);
				}
			});
		}
		public IExecutorService getExecutorService(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IExecutorService>() {
				public IExecutorService run() throws Exception {
					return hazelcast.getExecutorService(name);
				}
			});
		}
		public IdGenerator getIdGenerator(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IdGenerator>() {
				public IdGenerator run() throws Exception {
					return hazelcast.getIdGenerator(name);
				}
			});
		}
		public IAtomicLong getAtomicLong(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<IAtomicLong>() {
				public IAtomicLong run() throws Exception {
					return hazelcast.getAtomicLong(name);
				}
			});
		}
		public ICountDownLatch getCountDownLatch(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<ICountDownLatch>() {
				public ICountDownLatch run() throws Exception {
					return hazelcast.getCountDownLatch(name);
				}
			});
		}
		public ISemaphore getSemaphore(final String name) {
			return getInstanceProxy(new PrivilegedExceptionAction<ISemaphore>() {
				public ISemaphore run() throws Exception {
					return hazelcast.getSemaphore(name);
				}
			});
		}
		public Cluster getCluster() {
			return hazelcast.getCluster();
		}
		public String getName() {
			return hazelcast.getName();
		}
		public void shutdown() {
			throw new UnsupportedOperationException();
		}
		public void restart() {
			throw new UnsupportedOperationException();
		}
		public Collection<DistributedObject> getDistributedObjects() {
			return hazelcast.getDistributedObjects();
		}
        public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
            throw new UnsupportedOperationException();
        }
        public boolean removeDistributedObjectListener(String registrationId) {
            throw new UnsupportedOperationException();
        }
		public Config getConfig() {
			return hazelcast.getConfig();
		}
        public PartitionService getPartitionService() {
            return hazelcast.getPartitionService();
        }

        public ClientService getClientService() {
            return hazelcast.getClientService();
        }
        public LoggingService getLoggingService() {
			return hazelcast.getLoggingService();
		}
		public LifecycleService getLifecycleService() {
			throw new UnsupportedOperationException();
		}
	}
	
	private <T> T getInstanceProxy(PrivilegedExceptionAction<T> action) {
		try {
			SecurityUtil.setSecureCall();
			T instance = null;
			try {
                //TODO @ali come up with an idea
//				instance = node.securityContext.doAsPrivileged(subject, action);
			} finally {
				SecurityUtil.resetSecureCall();
			}
			return createInstanceProxy(instance);
		} catch (Exception e) {
            ExceptionUtil.rethrow(e);
			return null;
		}
	}
	
	private <T> T createInstanceProxy(Object instance) {
		final Object instanceProxy = Proxy.newProxyInstance(getClass().getClassLoader(), 
				getAllInterfaces(instance), new SecureInstanceProxy(instance));
		return (T) instanceProxy;
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
	
	private class SecureInstanceProxy implements InvocationHandler {
		private final Object instance;
		SecureInstanceProxy(Object instance) {
			super();
			this.instance = instance;
		}
		
		public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
			SecurityUtil.setSecureCall();
			try {
                return null;
                //TODO @ali come up with an idea
//				return node.securityContext.doAsPrivileged(subject, new PrivilegedExceptionAction<Object>() {
//					public Object run() throws Exception {
//						try {
//							return method.invoke(instance, args);
//						} catch (InvocationTargetException e) {
//							final Throwable cause = e.getCause();
//							if(cause instanceof Exception) {
//								throw (Exception) cause;
//							}
//							throw new Exception(cause);
//						}
//					}
//				});
			} finally {
				SecurityUtil.resetSecureCall();
			}
		}
	}
}