package com.pubsub.model;

public class Message<T> {

    final T value;

    public Message(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
