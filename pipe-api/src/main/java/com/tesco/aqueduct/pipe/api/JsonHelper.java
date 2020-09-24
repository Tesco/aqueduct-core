package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.IOException;
import java.util.List;

/**
 * Helper to use one way of parsing Message across the pipe projects
 */
public class JsonHelper {
    // it should not be public, is because of the way we limiting response size
    public static final ObjectMapper MAPPER = configureObjectMapper(new ObjectMapper());


    public static ObjectMapper configureObjectMapper(final ObjectMapper mapper) {
        return mapper.registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module())
            .registerModule(new ParameterNamesModule())

            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
            .enable(JsonParser.Feature.ALLOW_COMMENTS)

            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private static final CollectionType
        messageListType = MAPPER.getTypeFactory().constructCollectionType(List.class, Message.class);

    public static Message messageFromJson(final String json) throws IOException {
        return MAPPER.readValue(json, Message.class);
    }

    public static List<Message> messageFromJsonArray(final String json) throws IOException {
        return MAPPER.readValue(json, messageListType);
    }

    public static List<Message> messageFromJsonArray(final byte[] json) {
        try {
            return MAPPER.readValue(json, messageListType);
        } catch (final IOException ioException) {
            throw new RuntimeException("IO Error while mapping bytes to Messages", ioException);
        }
    }

    public static String toJson(final Object msg) throws IOException {
        return MAPPER.writeValueAsString(msg);
    }

    public static String toJson(final List<?> msg) {
        try {
            return MAPPER.writeValueAsString(msg);
        } catch (final JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("Json processing error while mapping messages", jsonProcessingException);
        }
    }

    public static byte[] toJsonBytes(final Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (final JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("Json processing error while mapping object", jsonProcessingException);
        }
    }
}
