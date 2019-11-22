package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.IOException;
import java.util.List;

public class JsonHelper {
    private JsonHelper() {}

    private static final ObjectMapper MAPPER = configureObjectMapper(new ObjectMapper());

    private static ObjectMapper configureObjectMapper(final ObjectMapper mapper) {
        return mapper.registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module())
            .registerModule(new ParameterNamesModule())

            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

            .enable(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
            .enable(JsonParser.Feature.ALLOW_COMMENTS)

            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static String toJson(final Object msg) throws IOException {
        return MAPPER.writeValueAsString(msg);
    }

    public static String toJson(final List<?> msg) throws IOException {
        return MAPPER.writeValueAsString(msg);
    }

    public static <T> List<T> listFromJson(String json, Class<T> collectionOf) throws IOException {
        CollectionType type = MAPPER.getTypeFactory().constructCollectionType(List.class, collectionOf);
        return MAPPER.readValue(json, type);
    }

    public static <T> T fromJson(String json, Class<T> typeOf) throws IOException {
        return MAPPER.readValue(json, typeOf);
    }
}
