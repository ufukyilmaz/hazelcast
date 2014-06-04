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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HazelcastSessionManager extends ManagerBase implements Lifecycle, PropertyChangeListener, SessionManager {

    private static final String NAME = "HazelcastSessionManager";
    private static final String INFO = "HazelcastSessionManager/1.0";

    private static final int DEFAULT_MAP_SIZE = 1000;
    private static final int DEFAULT_SESSION_TIMEOUT = 60;

    protected LifecycleSupport lifecycle = new LifecycleSupport(this);

    private final Log log = LogFactory.getLog(HazelcastSessionManager.class);

    private Map<String, HazelcastSession> localSessionMap = new ConcurrentHashMap<String, HazelcastSession>(DEFAULT_MAP_SIZE);

    private int rejectedSessions;
    private int maxActiveSessions = -1;

    private HazelcastInstance instance;

    private IMap<String, HazelcastSession> sessionMap;

    private boolean clientOnly;

    private boolean sticky;

    private String mapName;

    @Override
    public String getInfo() {
        return INFO;
    }

    @Override
    public int getRejectedSessions() {
        return rejectedSessions;
    }

    @Override
    public void setRejectedSessions(int i) {

    }

    @Override
    public String getName() {
        return NAME;
    }

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

        if (isClientOnly()) {
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

        if (!isSticky()) {
            sessionMap.addEntryListener(new EntryListener<String, HazelcastSession>() {
                public void entryAdded(EntryEvent<String, HazelcastSession> event) {

                }

                public void entryRemoved(EntryEvent<String, HazelcastSession> entryEvent) {
                    if (entryEvent.getMember() == null || !entryEvent.getMember().localMember()) {
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
    public Session createSession(String sessionId) {
        checkMaxActiveSessions();
        HazelcastSession session = (HazelcastSession) createEmptySession();

        session.setNew(true);
        session.setValid(true);
        session.setCreationTime(System.currentTimeMillis());
        session.setMaxInactiveInterval(getMaxInactiveInterval());

        String newSessionId = sessionId;
        if (newSessionId == null) {
            newSessionId = generateSessionId();
        }

        session.setId(newSessionId);
        session.tellNew();

        localSessionMap.put(newSessionId, session);
        sessionMap.put(newSessionId, session);
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
        log.info("sessionId:" + id);
        if (id == null) {
            return null;
        }

        if (localSessionMap.containsKey(id)) {
            return localSessionMap.get(id);
        }

        log.info("seems that sticky session is disabled or some failover occured.");
        // TODO check if session id is changed if failover occured...
        HazelcastSession hazelcastSession = sessionMap.get(id);
        log.info("Thread name:" + Thread.currentThread().getName() + "key:" + hazelcastSession.getAttribute("key"));

        hazelcastSession.setManager(this);

        localSessionMap.put(id, hazelcastSession);

        // call remove method to trigger eviction Listener on each node to invalidate local sessions
        sessionMap.remove(id);
        sessionMap.put(id, hazelcastSession);


        return hazelcastSession;
    }

    public void commit(Session session) {

        HazelcastSession hazelcastSession = (HazelcastSession) session;

        if (hazelcastSession.isDirty()) {
            hazelcastSession.setDirty(false);
            sessionMap.put(session.getId(), hazelcastSession);
            log.info("Thread name:" + Thread.currentThread().getName() + " commited key:" + hazelcastSession.getAttribute("key"));
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

        if (evt.getPropertyName().equals("sessionTimeout")) {
            setMaxInactiveInterval((Integer) evt.getNewValue() * DEFAULT_SESSION_TIMEOUT);
        }

    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }
    private void checkMaxActiveSessions() {
        if (getMaxActiveSessions() >= 0 && sessionMap.size() >= getMaxActiveSessions()) {
            rejectedSessions++;
            throw new IllegalStateException(sm.getString("standardManager.createSession.ise"));
        }
    }

    public int getMaxActiveSessions() {
        return this.maxActiveSessions;
    }

    public void setMaxActiveSessions(int maxActiveSessions) {
        int oldMaxActiveSessions = this.maxActiveSessions;
        this.maxActiveSessions = maxActiveSessions;
        this.support.firePropertyChange("maxActiveSessions",
                new Integer(oldMaxActiveSessions), new Integer(this.maxActiveSessions));
    }


}
