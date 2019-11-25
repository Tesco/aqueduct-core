package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.experimental.Wither;

import java.time.ZonedDateTime;
import java.util.Objects;

@Data
@Wither
public class Message {
    private final String type;
    private final String key;
    private final String contentType;
    private final Long offset;
    private final ZonedDateTime created;
    private final String data;
    @JsonIgnore private final Long size;

    private static final int MAX_OFFSET_LENGTH = 19;
    private static final int MAX_DATE_LENGTH = 64;
    private static final int EXTRA_ENCODING_CHARACTERS = 6;

    public static final int MAX_OVERHEAD_SIZE = MAX_OFFSET_LENGTH + MAX_DATE_LENGTH + EXTRA_ENCODING_CHARACTERS;

    @JsonCreator
    public Message(
        final String type,
        final String key,
        final String contentType,
        final Long offset,
        final ZonedDateTime created,
        final String data
    ) {
        this(type, key, contentType, offset, created, data, 0L);
    }

    public Message(
        final String type,
        final String key,
        final String contentType,
        final Long offset,
        final ZonedDateTime created,
        final String data,
        final Long size
    ) {
        this.offset = offset;
        this.key = key;
        this.type = type;
        this.contentType = contentType;
        this.created = created;
        this.data = data;
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(type, message.type) &&
            Objects.equals(key, message.key) &&
            Objects.equals(contentType, message.contentType) &&
            Objects.equals(offset, message.offset) &&
            (Objects.equals(created, message.created) || (created != null && created.isEqual(message.created))) &&
            Objects.equals(data, message.data) &&
            Objects.equals(size, message.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key, contentType, offset, created, data, size);
    }
}
