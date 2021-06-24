package io.kaleido.cordaconnector.model.common;

import net.corda.core.contracts.StateRef;

import java.time.Instant;

public class Event<T> {
    private T data;
    private String subId;
    private String signature;
    private StateRef stateRef;
    private Instant recordedTime;
    private Instant consumedTime;
    public Event(){

    }

    public Event(T data, String subId, String signature, StateRef stateRef, Instant recordedTime, Instant consumedTime) {
        this.data = data;
        this.subId = subId;
        this.signature = signature;
        this.stateRef = stateRef;
        this.recordedTime = recordedTime;
        this.consumedTime = consumedTime;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public StateRef getStateRef() {
        return stateRef;
    }

    public void setStateRef(StateRef stateRef) {
        this.stateRef = stateRef;
    }

    public Instant getRecordedTime() {
        return recordedTime;
    }

    public void setRecordedTime(Instant recordedTime) {
        this.recordedTime = recordedTime;
    }

    public Instant getConsumedTime() {
        return consumedTime;
    }

    public void setConsumedTime(Instant consumedTime) {
        this.consumedTime = consumedTime;
    }
}
