package com.hazelcast.enterprise.wan.impl;

public class FinalizableInteger implements FinalizerAware, Finalizable {
    private final int value;
    private volatile Finalizer finalizer;

    public FinalizableInteger(int value) {
        this.value = value;
    }

    @Override
    public void setFinalizer(Finalizer finalizer) {
        this.finalizer = finalizer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FinalizableInteger that = (FinalizableInteger) o;

        return value == that.value;
    }

    public static FinalizableInteger of(int value) {
        return new FinalizableInteger(value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    int intValue() {
        return value;
    }

    @Override
    public void doFinalize() {
        finalizer.doFinalize();
    }
}
