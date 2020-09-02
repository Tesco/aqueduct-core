package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.DistributedStorage;
import com.tesco.aqueduct.pipe.api.OffsetEntity;
import com.tesco.aqueduct.pipe.api.OffsetName;
import com.tesco.aqueduct.pipe.api.PipeEntity;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MetricizedDistributedStorage extends TimedDistributedStorage {

    private final Map<OffsetName, AtomicLong> atomicOffsets = new HashMap<>();

    public MetricizedDistributedStorage(final DistributedStorage storage, final MeterRegistry meterRegistry) {
        super(storage, meterRegistry);

        atomicOffsets.put(OffsetName.GLOBAL_LATEST_OFFSET, meterRegistry.gauge("pipe.offset.globalLatestOffset", new AtomicLong(0)));
        atomicOffsets.put(OffsetName.LOCAL_LATEST_OFFSET, meterRegistry.gauge("pipe.offset.localLatestOffset", new AtomicLong(0)));
        atomicOffsets.put(OffsetName.PIPE_OFFSET, meterRegistry.gauge("pipe.offset.pipeOffset", new AtomicLong(0)));
    }

    @Override
    public void write(OffsetEntity offset) {
        AtomicLong atomicOffset = atomicOffsets.get(offset.getName());
        atomicOffset.set(offset.getValue().getAsLong());
        super.write(offset);
    }

    @Override
    public void write(final PipeEntity pipeEntity) {
        if (pipeEntity.getOffsets() != null) {
            pipeEntity.getOffsets().forEach( offset -> {
                AtomicLong atomicOffset = atomicOffsets.get(offset.getName());
                atomicOffset.set(offset.getValue().getAsLong());
            });
        }
        super.write(pipeEntity);
    }
}
