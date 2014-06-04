package com.hazelcast.session;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

import java.security.Principal;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HazelcastSession extends StandardSession {

    protected boolean dirty;
    private Map<String, Object> localAttributeCache = new ConcurrentHashMap<String, Object>();
    private Map<String, Object> notes = new Hashtable<String, Object>();
    public HazelcastSession(Manager manager) {
    super(manager);
    }


  @Override
  public void setAttribute(String key, Object value) {
      dirty = true;
      localAttributeCache.put(key, value);
  }

    @Override
    public Object getAttribute(String name) {
       return localAttributeCache.get(name);
    }

    @Override
  public void removeAttribute(String name) {
    dirty = true;
        localAttributeCache.remove(name);
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public void setPrincipal(Principal principal) {
    dirty = true;
    super.setPrincipal(principal);
  }

    @Override
    public void invalidate() {
        super.setValid(false);
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public Object getNote(String name) {

        return (notes.get(name));

    }


    @Override
    public Iterator<String> getNoteNames() {

        return (notes.keySet().iterator());

    }

    @Override
    public void removeNote(String name) {

        notes.remove(name);

    }

    @Override
    public void setNote(String name, Object value) {

        notes.put(name, value);

    }



}
