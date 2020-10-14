package com.tesco.aqueduct.pipe.api;

import lombok.Data;

import java.util.List;
import java.util.OptionalLong;

@Data
public class MessageResults {
    private final List<Message> messages;
    private final long retryAfterMs;
    private final OptionalLong globalLatestOffset;
    private final PipeState pipeState;
}
