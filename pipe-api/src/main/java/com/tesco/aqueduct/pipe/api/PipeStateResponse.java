package com.tesco.aqueduct.pipe.api;

import lombok.Data;

@Data
public class PipeStateResponse {
    private final PipeState pipeState;
    private final long localOffset;

    public boolean isUpToDate() {
        return pipeState == PipeState.UP_TO_DATE;
    }
}
