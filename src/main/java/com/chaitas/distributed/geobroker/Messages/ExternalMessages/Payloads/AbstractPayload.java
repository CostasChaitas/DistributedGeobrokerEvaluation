// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.jetbrains.annotations.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@payloadType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CONNECTPayload.class, name = "CONNECTPayload"),
        @JsonSubTypes.Type(value = CONNACKPayload.class, name = "CONNACKPayload"),
        @JsonSubTypes.Type(value = DISCONNECTPayload.class, name = "DISCONNECTPayload"),
        @JsonSubTypes.Type(value = PINGREQPayload.class, name = "PINGREQPayload"),
        @JsonSubTypes.Type(value = PINGRESPPayload.class, name = "PINGRESPPayload"),
        @JsonSubTypes.Type(value = SUBSCRIBEPayload.class, name = "SUBSCRIBEPayload"),
        @JsonSubTypes.Type(value = UNSUBSCRIBEPayload.class, name = "UNSUBSCRIBEPayload"),
        @JsonSubTypes.Type(value = PUBLISHPayload.class, name = "PUBLISHPayload"),
        @JsonSubTypes.Type(value = SUBACKPayload.class, name = "SUBACKPayload"),
        @JsonSubTypes.Type(value = PUBACKPayload.class, name = "PUBACKPayload"),
        @JsonSubTypes.Type(value = PUBACKPayload.class, name = "INCOMPATIBLEPayload")
})
public abstract class AbstractPayload {

    public final CONNECTPayload getCONNECTPayload() {
        return this instanceof CONNECTPayload ? (CONNECTPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final CONNACKPayload getCONNACKPayload() {
        return this instanceof CONNACKPayload ? (CONNACKPayload)this : null;
    }

    public final DISCONNECTPayload getDISCONNECTPayload() {
        return this instanceof DISCONNECTPayload ? (DISCONNECTPayload)this : null;
    }

    public final PINGREQPayload getPINGREQPayload() {
        return this instanceof PINGREQPayload ? (PINGREQPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final PINGRESPPayload getPINGRESPPayload() {
        return this instanceof PINGRESPPayload ? (PINGRESPPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final PUBLISHPayload getPUBLISHPayload() {
        return this instanceof PUBLISHPayload ? (PUBLISHPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final PUBACKPayload getPUBACKPayload() {
        return this instanceof PUBACKPayload ? (PUBACKPayload)this : null;
    }

    public final SUBSCRIBEPayload getSUBSCRIBEPayload() {
        return this instanceof SUBSCRIBEPayload ? (SUBSCRIBEPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final SUBACKPayload getSUBACKPayload() {
        return this instanceof SUBACKPayload ? (SUBACKPayload)this : null;
    }

    public final UNSUBSCRIBEPayload getUNSUBSCRIBEPayload() {
        return this instanceof UNSUBSCRIBEPayload ? (UNSUBSCRIBEPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final UNSUBACKPayload getUNSUBACKPayload() {
        return this instanceof UNSUBACKPayload ? (UNSUBACKPayload)this : null;
    }

    @JsonIgnore
    @Nullable
    public final INCOMPATIBLEPayload getINCOMPATIBLEPayload() {
        return this instanceof INCOMPATIBLEPayload ? (INCOMPATIBLEPayload)this : null;
    }

}
