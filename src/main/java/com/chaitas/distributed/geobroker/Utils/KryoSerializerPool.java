package com.chaitas.distributed.geobroker.Utils;


import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.*;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ReasonCode;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

import java.io.ByteArrayOutputStream;

public class KryoSerializerPool {

    private KryoPool pool;

    public KryoSerializerPool(){

        KryoFactory factory = () -> {

            Kryo kryo = new Kryo();

            kryo.register(Topic.class, new Serializer<Topic>(){
                public void write (Kryo kryo, Output output, Topic object) {
                    kryo.writeObjectOrNull(output, object.getTopic(), String.class);
                }

                public Topic read (Kryo kryo, Input input, Class<Topic> type) {
                    String topic = kryo.readObjectOrNull(input, String.class);
                    return new Topic(topic);
                }
            });

            kryo.register(Location.class, new Serializer<Location>() {
                public void write (Kryo kryo, Output output, Location object) {
                    if(object.isUndefined()){
                        kryo.writeObjectOrNull(output, -1000.0, Double.class);
                        kryo.writeObjectOrNull(output, -1000.0, Double.class);
                    } else {
                        kryo.writeObjectOrNull(output, object.getLat(), Double.class);
                        kryo.writeObjectOrNull(output, object.getLon(), Double.class);
                    }
                }

                public Location read (Kryo kryo, Input input, Class<Location> type) {
                    Double lat = kryo.readObjectOrNull(input, Double.class);
                    Double lon = kryo.readObjectOrNull(input, Double.class);
                    if (lat == -1000.0 && lon == -1000.0) {
                        return new Location(true);
                    } else {
                        return new Location(lat, lon);
                    }
                }
            });

            kryo.register(Geofence.class, new Serializer<Geofence>() {
                public void write (Kryo kryo, Output output, Geofence object) {
                    kryo.writeObjectOrNull(output, object.getWKT(), String.class);
                }

                public Geofence read (Kryo kryo, Input input, Class<Geofence> type) {
                    try {
                        String wktString = kryo.readObjectOrNull(input, String.class);
                        return new Geofence(wktString);
                    } catch (Exception ex) {
                        throw new Error(ex);
                    }
                }
            });

            kryo.register(CONNACKPayload.class, new Serializer<CONNACKPayload>() {
                public void write (Kryo kryo, Output output, CONNACKPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public CONNACKPayload read (Kryo kryo, Input input, Class<CONNACKPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new CONNACKPayload(reasonCode);
                }
            });

            kryo.register(CONNECTPayload.class, new Serializer<CONNECTPayload>() {
                public void write (Kryo kryo, Output output, CONNECTPayload object) {
                    kryo.writeObjectOrNull(output, object.location, Location.class);
                }

                public CONNECTPayload read (Kryo kryo, Input input, Class<CONNECTPayload> type) {
                    Location location = kryo.readObjectOrNull(input, Location.class);
                    return new CONNECTPayload(location);
                }
            });

            kryo.register(DISCONNECTPayload.class, new Serializer<DISCONNECTPayload>() {
                public void write (Kryo kryo, Output output, DISCONNECTPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public DISCONNECTPayload read (Kryo kryo, Input input, Class<DISCONNECTPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new DISCONNECTPayload(reasonCode);
                }
            });

            kryo.register(PINGREQPayload.class, new Serializer<PINGREQPayload>() {
                public void write (Kryo kryo, Output output, PINGREQPayload object) {
                    kryo.writeObjectOrNull(output, object.location, Location.class);
                }

                public PINGREQPayload read (Kryo kryo, Input input, Class<PINGREQPayload> type) {
                    Location location = kryo.readObjectOrNull(input, Location.class);
                    return new PINGREQPayload(location);
                }
            });

            kryo.register(PINGRESPPayload.class, new Serializer<PINGRESPPayload>() {
                public void write (Kryo kryo, Output output, PINGRESPPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public PINGRESPPayload read (Kryo kryo, Input input, Class<PINGRESPPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new PINGRESPPayload(reasonCode);
                }
            });

            kryo.register(PUBACKPayload.class, new Serializer<PUBACKPayload>() {
                public void write (Kryo kryo, Output output, PUBACKPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public PUBACKPayload read (Kryo kryo, Input input, Class<PUBACKPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new PUBACKPayload(reasonCode);
                }
            });

            kryo.register(PUBLISHPayload.class, new Serializer<PUBLISHPayload>() {
                public void write (Kryo kryo, Output output, PUBLISHPayload object) {
                    kryo.writeObjectOrNull(output, object.topic, Topic.class);
                    kryo.writeObjectOrNull(output, object.geofence, Geofence.class);
                    kryo.writeObjectOrNull(output, object.content, String.class);
                }

                public PUBLISHPayload read (Kryo kryo, Input input, Class<PUBLISHPayload> type) {
                    Topic topic = kryo.readObjectOrNull(input, Topic.class);
                    Geofence geofence = kryo.readObjectOrNull(input, Geofence.class);
                    String content = kryo.readObjectOrNull(input, String.class);
                    return new PUBLISHPayload(topic, geofence, content);
                }
            });

            kryo.register(SUBACKPayload.class, new Serializer<SUBACKPayload>() {
                public void write (Kryo kryo, Output output, SUBACKPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public SUBACKPayload read (Kryo kryo, Input input, Class<SUBACKPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new SUBACKPayload(reasonCode);
                }
            });

            kryo.register(SUBSCRIBEPayload.class, new Serializer<SUBSCRIBEPayload>() {
                public void write (Kryo kryo, Output output, SUBSCRIBEPayload object) {
                    kryo.writeObjectOrNull(output, object.topic, Topic.class);
                    kryo.writeObjectOrNull(output, object.geofence, Geofence.class);
                }

                public SUBSCRIBEPayload read (Kryo kryo, Input input, Class<SUBSCRIBEPayload> type) {
                    Topic topic = kryo.readObjectOrNull(input, Topic.class);
                    Geofence geofence = kryo.readObjectOrNull(input, Geofence.class);
                    return new SUBSCRIBEPayload(topic, geofence);
                }
            });

            kryo.register(UNSUBACKPayload.class, new Serializer<UNSUBACKPayload>() {
                public void write (Kryo kryo, Output output, UNSUBACKPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public UNSUBACKPayload read (Kryo kryo, Input input, Class<UNSUBACKPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new UNSUBACKPayload(reasonCode);
                }
            });

            kryo.register(UNSUBSCRIBEPayload.class, new Serializer<UNSUBSCRIBEPayload>() {
                public void write (Kryo kryo, Output output, UNSUBSCRIBEPayload object) {
                    kryo.writeObjectOrNull(output, object.topic, Topic.class);
                }

                public UNSUBSCRIBEPayload read (Kryo kryo, Input input, Class<UNSUBSCRIBEPayload> type) {
                    Topic topic = kryo.readObjectOrNull(input, Topic.class);
                    return new UNSUBSCRIBEPayload(topic);
                }
            });

            kryo.register(INCOMPATIBLEPayload.class, new Serializer<INCOMPATIBLEPayload>() {
                public void write (Kryo kryo, Output output, INCOMPATIBLEPayload object) {
                    kryo.writeObjectOrNull(output, object.reasonCode, ReasonCode.class);
                }

                public INCOMPATIBLEPayload read (Kryo kryo, Input input, Class<INCOMPATIBLEPayload> type) {
                    ReasonCode reasonCode = kryo.readObjectOrNull(input, ReasonCode.class);
                    return new INCOMPATIBLEPayload(reasonCode);
                }
            });

            kryo.register(ExternalMessage.class, new Serializer<ExternalMessage>() {
                public void write (Kryo kryo, Output output, ExternalMessage object) {
                    kryo.writeObjectOrNull(output, object.getClientIdentifier(), String.class);
                    kryo.writeObjectOrNull(output, object.getControlPacketType(), ControlPacketType.class);
                    switch(object.getControlPacketType()){
                        case CONNACK:
                            kryo.writeObjectOrNull(output, object.getPayload(), CONNACKPayload.class);
                            break;
                        case CONNECT:
                            kryo.writeObjectOrNull(output, object.getPayload(), CONNECTPayload.class);
                            break;
                        case DISCONNECT:
                            kryo.writeObjectOrNull(output, object.getPayload(), DISCONNECTPayload.class);
                            break;
                        case PINGREQ:
                            kryo.writeObjectOrNull(output, object.getPayload(), PINGREQPayload.class);
                            break;
                        case PINGRESP:
                            kryo.writeObjectOrNull(output, object.getPayload(), PINGRESPPayload.class);
                            break;
                        case PUBACK:
                            kryo.writeObjectOrNull(output, object.getPayload(), PUBACKPayload.class);
                            break;
                        case PUBLISH:
                            kryo.writeObjectOrNull(output, object.getPayload(), PUBLISHPayload.class);
                            break;
                        case SUBACK:
                            kryo.writeObjectOrNull(output, object.getPayload(), SUBACKPayload.class);
                            break;
                        case SUBSCRIBE:
                            kryo.writeObjectOrNull(output, object.getPayload(), SUBSCRIBEPayload.class);
                            break;
                        case UNSUBACK:
                            kryo.writeObjectOrNull(output, object.getPayload(), UNSUBACKPayload.class);
                            break;
                        case UNSUBSCRIBE:
                            kryo.writeObjectOrNull(output, object.getPayload(), UNSUBSCRIBEPayload.class);
                            break;
                        case INCOMPATIBLEPayload:
                            kryo.writeObjectOrNull(output, object.getPayload(), INCOMPATIBLEPayload.class);
                            break;
                        default:
                            kryo.writeObjectOrNull(output, object.getPayload(), INCOMPATIBLEPayload.class);
                            break;
                    }
                    return;
                }

                public ExternalMessage read (Kryo kryo, Input input, Class<ExternalMessage> type) {
                    String clientIdentifier = kryo.readObjectOrNull(input, String.class);
                    ControlPacketType controlPacketType = kryo.readObjectOrNull(input, ControlPacketType.class);
                    switch(controlPacketType){
                        case CONNACK:
                            CONNACKPayload connackPayload = kryo.readObjectOrNull(input, CONNACKPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, connackPayload);
                        case CONNECT:
                            CONNECTPayload connectPayload = kryo.readObjectOrNull(input, CONNECTPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, connectPayload);
                        case DISCONNECT:
                            DISCONNECTPayload disconnectPayload = kryo.readObjectOrNull(input, DISCONNECTPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, disconnectPayload);
                        case PINGREQ:
                            PINGREQPayload pingreqPayload = kryo.readObjectOrNull(input, PINGREQPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, pingreqPayload);
                        case PINGRESP:
                            PINGRESPPayload pingrespPayload = kryo.readObjectOrNull(input, PINGRESPPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, pingrespPayload);
                        case PUBACK:
                            PUBACKPayload pubackPayload = kryo.readObjectOrNull(input, PUBACKPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, pubackPayload);
                        case PUBLISH:
                            PUBLISHPayload publishPayload = kryo.readObjectOrNull(input, PUBLISHPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, publishPayload);
                        case SUBACK:
                            SUBACKPayload subackPayload = kryo.readObjectOrNull(input, SUBACKPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, subackPayload);
                        case SUBSCRIBE:
                            SUBSCRIBEPayload subscribePayload = kryo.readObjectOrNull(input, SUBSCRIBEPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, subscribePayload);
                        case UNSUBACK:
                            UNSUBACKPayload unsubackPayload = kryo.readObjectOrNull(input, UNSUBACKPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, unsubackPayload);
                        case UNSUBSCRIBE:
                            UNSUBSCRIBEPayload unsubscribePayload = kryo.readObjectOrNull(input, UNSUBSCRIBEPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, unsubscribePayload);
                        case INCOMPATIBLEPayload:
                            INCOMPATIBLEPayload incompatiblePayload = kryo.readObjectOrNull(input, INCOMPATIBLEPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, incompatiblePayload);
                        default:
                            INCOMPATIBLEPayload incompatiblePayload1 = kryo.readObjectOrNull(input, INCOMPATIBLEPayload.class);
                            return new ExternalMessage(clientIdentifier, controlPacketType, incompatiblePayload1);
                    }
                }
            });
            return kryo;
        };

        pool = new KryoPool.Builder(factory).softReferences().build();

    }

    public byte[] write(Object obj) {
        Output output = new Output(new ByteArrayOutputStream(), 10024);
        Kryo kryo = pool.borrow();
        kryo.writeObject(output, obj);
        byte[] serialized = output.toBytes();
        pool.release(kryo);
        return serialized;
    }

    public Object read(byte[] byteArray, Class targetClass) {
        Object obj;
        Kryo kryo = pool.borrow();
        Input input = new Input(byteArray);
        obj = kryo.readObject(input, targetClass);
        pool.release(kryo);
        return obj;
    }

}
