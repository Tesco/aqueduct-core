package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public class CloudPipeStateProvider implements PipeStateProvider {
    @Override
    public PipeStateResponse getState(List<String> types, MessageReader reader) {
        return new PipeStateResponse(true, reader.getLatestOffsetMatching(types));
    }
}
