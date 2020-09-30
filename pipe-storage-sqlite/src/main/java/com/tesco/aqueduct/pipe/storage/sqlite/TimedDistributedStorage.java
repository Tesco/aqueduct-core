package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.OptionalLong;

public class TimedDistributedStorage implements DistributedStorage {
    private final DistributedStorage storage;
    private final LongTaskTimer readTimer;
    private final LongTaskTimer writeMessageTimer;
    private final LongTaskTimer writeMessagesTimer;
    private final LongTaskTimer writePipeStateTimer;
    private final LongTaskTimer readPipeStateTimer;
    private final LongTaskTimer readOffsetTimer;
    private final LongTaskTimer writeOffsetTimer;

    public TimedDistributedStorage(final DistributedStorage storage, final MeterRegistry meterRegistry) {
        this.storage = storage;
        readTimer = meterRegistry.more().longTaskTimer("pipe.storage.read");
        readOffsetTimer = meterRegistry.more().longTaskTimer("pipe.storage.readOffset");
        writeOffsetTimer = meterRegistry.more().longTaskTimer("pipe.storage.writeOffset");
        writeMessageTimer = meterRegistry.more().longTaskTimer("pipe.storage.writeMessage");
        writeMessagesTimer = meterRegistry.more().longTaskTimer("pipe.storage.writeMessages");
        writePipeStateTimer = meterRegistry.more().longTaskTimer("pipe.storage.writePipeState");
        readPipeStateTimer = meterRegistry.more().longTaskTimer("pipe.storage.readPipeState");
    }

    @Override
    public MessageResults read(final List<String> types, final long offset, final List<String> locationUuids) {
        return readTimer.record(() -> storage.read(types, offset, locationUuids));
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return readOffsetTimer.record(() -> storage.getOffset(offsetName));
    }

    @Override
    public void write(final Iterable<Message> messages) {
        writeMessageTimer.record(() -> storage.write(messages));
    }

    @Override
    public void write(PipeEntity pipeEntity) {
        writeMessagesTimer.record(() -> storage.write(pipeEntity));
    }

    @Override
    public void write(final Message message) {
        writeMessagesTimer.record(() -> storage.write(message));
    }

    @Override
    public void write(OffsetEntity offset) {
        writeOffsetTimer.record(() -> storage.write(offset));
    }

    @Override
    public void write(PipeState pipeState) {
        writePipeStateTimer.record(() -> storage.write(pipeState));
    }

    @Override
    public void deleteAll() {
        storage.deleteAll();
    }

    @Override
    public PipeState getPipeState() {
        return readPipeStateTimer.record(storage::getPipeState);
    }
}
