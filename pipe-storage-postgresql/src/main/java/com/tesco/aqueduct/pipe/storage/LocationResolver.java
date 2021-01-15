package com.tesco.aqueduct.pipe.storage;

import java.util.List;
import java.util.Optional;

public interface LocationResolver {

    Optional<List<Long>> getClusterIds(String locationUuid);
}
