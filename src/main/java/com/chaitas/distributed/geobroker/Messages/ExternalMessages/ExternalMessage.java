// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.AbstractPayload;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ExternalMessage {
    private String clientIdentifier;
    private ControlPacketType controlPacketType;
    private AbstractPayload payload;

    public ExternalMessage(
            @JsonProperty("clientIdentifier") String clientIdentifier,
            @JsonProperty("controlPacketType") ControlPacketType controlPacketType,
            @JsonProperty("payload") AbstractPayload payload) {
        this.clientIdentifier = clientIdentifier;
        this.controlPacketType = controlPacketType;
        this.payload = payload;
    }

    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public ControlPacketType getControlPacketType() {
        return controlPacketType;
    }

    public AbstractPayload getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "ExternalMessage{" +
                "clientIdentifier='" + clientIdentifier + '\'' +
                ", controlPacketType=" + controlPacketType +
                ", payload=" + payload +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalMessage)) {
            return false;
        }
        ExternalMessage that = (ExternalMessage) o;
        return Objects.equals(getClientIdentifier(), that.getClientIdentifier()) &&
                getControlPacketType() == that.getControlPacketType() &&
                Objects.equals(getPayload(), that.getPayload());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClientIdentifier(), getControlPacketType(), getPayload());
    }
}
