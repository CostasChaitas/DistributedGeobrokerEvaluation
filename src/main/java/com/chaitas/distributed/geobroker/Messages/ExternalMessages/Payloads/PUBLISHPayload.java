// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

public class PUBLISHPayload extends AbstractPayload {

    public Topic topic;
    public Geofence geofence;
    public String content;

    public PUBLISHPayload(@NotNull @JsonProperty("topic") Topic topic,
                          @NotNull @JsonProperty("geofence") Geofence geofence,
                          @NotNull @JsonProperty("content") String content){
        this.topic = topic;
        this.geofence = geofence;
        this.content = content;
    }

    public Topic getTopic() {
        return topic;
    }

    public Geofence getGeofence() {
        return geofence;
    }

    public String getContent() {
        return content;
    }
}
