package com.hazelcast.session;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HazelcastSessionManager extends ManagerBase implements Lifecycle,PropertyChangeListener,SessionManager {

    private final Log log = LogFactory.getLog(com.hazelcast.session.HazelcastSessionManager.class);

    Map<String,HazelcastSession> localSessionMap = new ConcurrentHashMap<String,HazelcastSession>(1000);

    private static String NAME = "HazelcastSessionManager";
    private static String INFO = "HazelcastSessionManager/1.0";

    HazelcastInstance instance;

    IMap<String, HazelcastSession> sessionMap;
    List<String> invalidSessions;

    private boolean clientOnly;

    private boolean sticky;

    private String mapName;

    @Override
    public String getInfo() {
        return INFO;
    }

    @Override
    public String getName() {
        return NAME;
    }

    protected LifecycleSupport lifecycle = new LifecycleSupport(this);

    @Override
    public void load() throws ClassNotFoundException, IOException {

    }

    @Override
    public void unload() throws IOException {

    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        lifecycle.addLifecycleListener(listener);
    }

    @Override
    public LifecycleListener[] findLifecycleListeners() {
        return lifecycle.findLifecycleListeners();
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        lifecycle.removeLifecycleListener(listener);
    }

    @Override
    public void start() throws LifecycleException {

        init();
        lifecycle.fireLifecycleEvent(START_EVENT, null);

        if (log.isDebugEnabled()) {
            log.debug("Force random number initialization starting");
        }
        super.generateSessionId();
        if (log.isDebugEnabled()) {
            log.debug("Force random number initialization completed");
        }

        HazelcastSessionCommitValve hazelcastSessionCommitValve = new HazelcastSessionCommitValve(this);

        getContainer().getPipeline().addValve(hazelcastSessionCommitValve);

        if (isClientOnly()){
            //TODO read client config from filesystem,classpath etc.
            //TODO throw lifecycleevent in case you can not connect to an existing cluster.
            instance = HazelcastClient.newHazelcastClient();
        } else {
            //TODO read client config from filesystem,classpath etc.
            instance = Hazelcast.newHazelcastInstance();
        }
        if (getMapName() == null) {
            sessionMap = instance.getMap("default");
        } else {
            sessionMap = instance.getMap(getMapName());
        }

        if (!isSticky()){
            sessionMap.addEntryListener(new EntryListener<String, HazelcastSession>() {
                public void entryAdded(EntryEvent<String, HazelcastSession> event) {

                }

                public void entryRemoved(EntryEvent<String, HazelcastSession> entryEvent) {
                    if (entryEvent.getMember() == null || // client events has no owner member
                            !entryEvent.getMember().localMember()) {
                        localSessionMap.remove(entryEvent.getKey());
                    }
                }

                public void entryUpdated(EntryEvent<String, HazelcastSession> event) {

                }

                public void entryEvicted(EntryEvent<String, HazelcastSession> entryEvent) {
                    entryRemoved(entryEvent);
                }
            }, false);

        }


        log.info("HazelcastSessionManager started...");

    }


    @Override
    public void stop() throws LifecycleException {

        lifecycle.fireLifecycleEvent(STOP_EVENT, null);

        instance.shutdown();
        log.info("HazelcastSessionManager stopped...");
    }


    @Override
    public int getRejectedSessions() {
        // Essentially do nothing.
        return 0;
    }

    public void setRejectedSessions(int i) {
        // Do nothing.
    }


    @Override
    public Session createSession(String sessionId) {
        HazelcastSession session = (HazelcastSession) createEmptySession();

        session.setNew(true);
        session.setValid(true);
        session.setCreationTime(System.currentTimeMillis());
        session.setMaxInactiveInterval(getMaxInactiveInterval());

        if (sessionId == null) {
            sessionId = generateSessionId();
        }

        session.setId(sessionId);
        session.tellNew();

        localSessionMap.put(sessionId,session);
        sessionMap.put(sessionId, session);
        return session;
    }

    @Override
    public Session createEmptySession() {
        return new HazelcastSession(this);
    }

    @Override
    public void add(Session session) {
        localSessionMap.put(session.getId(), (HazelcastSession) session);
        sessionMap.put(session.getId(), (HazelcastSession) session);
    }

    @Override
    public Session findSession(String id) throws IOException {
        log.info("sessionId:"+id);
        if (id == null) {
            return null;
        }

        if (localSessionMap.containsKey(id)) {
            return localSessionMap.get(id);
        }

        log.info("seems that sticky session is disabled or some failover occured.");
        // TODO check if session id is changed if failover occured...
        HazelcastSession hazelcastSession = sessionMap.get(id);
        log.info("Thread name:"+Thread.currentThread().getName()+"key:"+hazelcastSession.getAttribute("key"));

        hazelcastSession.setManager(this);

        localSessionMap.put(id,hazelcastSession);

        // call remove method to trigger eviction Listener on each node to invalidate local sessions
        sessionMap.remove(id);
        sessionMap.put(id, hazelcastSession);


        return hazelcastSession;
    }

    public void commit(Session session) {

        HazelcastSession hazelcastSession = (HazelcastSession) session;

        if (hazelcastSession.isDirty()){
            hazelcastSession.setDirty(false);
            sessionMap.put(session.getId(), hazelcastSession);
            log.info("Thread name:"+Thread.currentThread().getName()+" commited key:" + hazelcastSession.getAttribute("key"));
        }
    }

    @Override
    public void remove(Session session) {
        remove(session.getId());
   }

    @Override
    public void processExpires() {
        // Hazelcast TTL will do that
    }


    private IMap<String, HazelcastSession> getSessionMap() {
        return sessionMap;
    }

    public boolean isClientOnly() {
        return clientOnly;
    }

    public void setClientOnly(boolean clientOnly) {
        this.clientOnly = clientOnly;
    }

    public boolean isSticky() {
        return sticky;
    }

    public void setSticky(boolean sticky) {
        this.sticky = sticky;
    }

    private void remove(String id) {
        localSessionMap.remove(id);
        sessionMap.remove(id);
    }

    @Override
    public void expireSession(String sessionId) {
        remove(sessionId);
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {

       if (evt.getPropertyName().equals("sessionTimeout")){
            setMaxInactiveInterval((Integer)evt.getNewValue() * 60);
        }

    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }


}
