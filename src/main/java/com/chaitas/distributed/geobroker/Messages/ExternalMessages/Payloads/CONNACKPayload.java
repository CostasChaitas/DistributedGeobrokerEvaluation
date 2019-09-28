// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ReasonCode;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

public class CONNACKPayload extends AbstractPayload {

    public ReasonCode reasonCode;

    public CONNACKPayload(@NotNull @JsonProperty("reasonCode") ReasonCode reasonCode){
        this.reasonCode = reasonCode;
    }
}
