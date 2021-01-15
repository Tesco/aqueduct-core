package com.tesco.aqueduct.pipe.storage;

import java.util.List;

public interface LocationResolver {

    List<Long> getClusterIds(String locationUuid);
}
