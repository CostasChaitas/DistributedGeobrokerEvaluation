// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class JSONable {

    static ObjectMapper mapper = new ObjectMapper();

    static AtomicBoolean configured = new AtomicBoolean(false);

    static public <T> Optional<T> fromJSON(String json, Class<T> targetClass) {
        try {
            return Optional.of(mapper.readValue(json, targetClass));
        } catch (Exception e) {
            System.out.println("Could not translate json to " + targetClass.getName() +  e);
            return Optional.empty();
        }
    }

    static public <T> Optional<T> fromJSON(InputStream stream, Class<T> targetClass) {
        try {
            return Optional.of(mapper.readValue(stream, targetClass));
        } catch (Exception e) {
            System.out.println("Could not translate json to " + targetClass.getName() + e);
            return Optional.empty();
        }
    }

    static public <T> String toJSON(T obj) {
        // configure if necessary
        if (!configured.get()) {
            // Only include non-null and non-empty fields
            mapper.setSerializationInclusion(Include.NON_NULL).setSerializationInclusion(Include.NON_EMPTY);
            mapper.registerModule(new Jdk8Module());
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            configured.set(true);
        }

        String json = null;
        try {
            json = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            System.out.println("Could not create JSON from object" +  e);
        }
        return json;
    }

    @SuppressWarnings({"ConstantConditions", "unchecked"})
    static public <T> T clone(JSONable object) {
        if (object == null) {
            return null;
        }

        String json = toJSON(object);
        JSONable clone = fromJSON(json, object.getClass()).get();

        return (T) clone;
    }

}