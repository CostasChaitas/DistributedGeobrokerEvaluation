// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

public class CONNECTPayload extends AbstractPayload {

    public Location location;

    public CONNECTPayload(@NotNull @JsonProperty("location") Location location){
        this.location = location;
    }
}
