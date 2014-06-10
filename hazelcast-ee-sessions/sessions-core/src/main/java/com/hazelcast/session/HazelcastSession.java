package com.hazelcast.session;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Map;


public class HazelcastSession extends StandardSession implements DataSerializable {

    protected boolean dirty;

    public HazelcastSession(Manager manager) {
        super(manager);
    }

    public HazelcastSession() {
        super(null);
    }

    @Override
  public void setAttribute(String key, Object value) {
      dirty = true;
      super.setAttribute(key, value);
  }


    @Override
  public void removeAttribute(String name) {
    dirty = true;
        super.removeAttribute(name);
  }

  @Override
  public void setPrincipal(Principal principal) {
    dirty = true;
    super.setPrincipal(principal);
  }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }


    public Map<String, Object> getLocalAttributeCache() {
        return attributes;
    }


    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeLong(creationTime);
        objectDataOutput.writeLong(lastAccessedTime);
        objectDataOutput.writeInt(maxInactiveInterval);
        objectDataOutput.writeBoolean(isNew);
        objectDataOutput.writeBoolean(isValid);
        objectDataOutput.writeLong(thisAccessedTime);
        objectDataOutput.writeObject(id);
        objectDataOutput.writeObject(attributes);
        objectDataOutput.writeObject(notes);

    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
       this.creationTime = objectDataInput.readLong();
       this.lastAccessedTime = objectDataInput.readLong();
       this.maxInactiveInterval = objectDataInput.readInt();
       this.isNew = objectDataInput.readBoolean();
       this.isValid = objectDataInput.readBoolean();
       this.thisAccessedTime = objectDataInput.readLong();
       this.id = objectDataInput.readObject();
       this.attributes = objectDataInput.readObject();
        this.notes = objectDataInput.readObject();

        if (this.listeners == null) {
            this.listeners = new ArrayList();
        }

    }
}
