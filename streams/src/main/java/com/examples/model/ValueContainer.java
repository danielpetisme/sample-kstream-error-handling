package com.examples.model;

public class ValueContainer<V> {

    private V value;
    private String exception;

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }
}
