package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;
import lombok.val;

import java.util.List;
import java.util.OptionalLong;

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET;

public class DistributedInMemoryStorage extends InMemoryStorage implements DistributedStorage {
    public DistributedInMemoryStorage(int limit, long retryAfter) {
        super(limit, retryAfter);
    }

    @Override
    protected OptionalLong getLatestGlobalOffset() {
        return offsets.containsKey(GLOBAL_LATEST_OFFSET) ?
            OptionalLong.of(offsets.get(GLOBAL_LATEST_OFFSET)) : OptionalLong.empty();
    }

    @Override
    public void write(PipeEntity pipeEntity) {
        if (pipeEntity != null) {
            if (pipeEntity.getMessages() != null) {
                pipeEntity.getMessages().forEach(this::write);
            }
            if (pipeEntity.getOffsets() != null) {
                pipeEntity.getOffsets().forEach(this::write);
            }
            if (pipeEntity.getPipeState() != null) {
                write(pipeEntity.getPipeState());
            }
        }
    }

    @Override
    public void write(OffsetEntity offset) {
        final val lock = rwl.writeLock();

        LOG.withOffset(offset).info("in memory storage", "writing offset");

        try {
            lock.lock();
            offsets.put(offset.getName(), offset.getValue().getAsLong());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(PipeState pipeState) {
        final val lock = rwl.writeLock();

        LOG.withPipeState(pipeState).info("in memory storage", "writing pipe state");

        try {
            lock.lock();
            this.pipeState = pipeState;
        } finally {
            lock.unlock();
        }
    }

    @Override
    boolean messageMatchCluster(Message message, List<String> clusterId) {
        return true;
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return offsets.get(offsetName) == null ? OptionalLong.empty() : OptionalLong.of(offsets.get(offsetName));
    }

    @Override
    public PipeState getPipeState() {
        return pipeState;
    }
}
