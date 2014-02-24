package com.hazelcast.examples;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

/**
* @author mdogan 18/12/13
*/
public final class SampleValue implements IdentifiedDataSerializable {
    private int i;
    private long l;
    private String s;
    private long[] ll;

    public SampleValue() {
    }

    public SampleValue(int i, long l, String s, long[] ll) {
        this.i = i;
        this.l = l;
        this.s = s;
        this.ll = ll;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getId() {
        return 1;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(i);
        out.writeUTF(s);
        out.writeLongArray(ll);
        out.writeLong(l);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        i = in.readInt();
        s = in.readUTF();
        ll = in.readLongArray();
        l = in.readLong();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        out.writeInt(i);
        out.writeUTF(s);
        int len = ll != null ? ll.length : 0;
        out.writeInt(len);
        if (len > 0) {
            for (int j = 0; j < len; j++) {
                out.writeLong(ll[j]);
            }
        }
        out.writeLong(l);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        i = in.readInt();
        s = in.readUTF();
        int len = in.readInt();
        if (len > 0) {
            ll = new long[len];
            for (int j = 0; j < len; j++) {
                ll[j] = in.readLong();
            }
        }
        l = in.readLong();

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Value{");
        sb.append("i=").append(i);
        sb.append(", l=").append(l);
        sb.append(", s='").append(s).append('\'');
        sb.append(", ll=").append(Arrays.toString(ll));
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SampleValue value = (SampleValue) o;

        if (i != value.i) return false;
        if (l != value.l) return false;
        if (!Arrays.equals(ll, value.ll)) return false;
        if (s != null ? !s.equals(value.s) : value.s != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        result = 31 * result + (s != null ? s.hashCode() : 0);
        result = 31 * result + (ll != null ? Arrays.hashCode(ll) : 0);
        return result;
    }
}
