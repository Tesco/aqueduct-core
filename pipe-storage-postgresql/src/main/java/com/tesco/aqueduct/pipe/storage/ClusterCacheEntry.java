package com.tesco.aqueduct.pipe.storage;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
public class ClusterCacheEntry {
    private final String locationUuid;
    private final List<Long> clusterIds;
    private final LocalDateTime expiry;
    private final boolean isValid;

    public boolean isValidAndUnexpired() {
        return isValid && expiry.isAfter(LocalDateTime.now());
    }
}
