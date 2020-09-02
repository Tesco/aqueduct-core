package com.tesco.aqueduct.pipe.api;

import lombok.Value;

import java.util.List;

@Value
public class PipeEntity {
    List<Message> messages;
    List<OffsetEntity> offsets;
    PipeState pipeState;
}
