package com.tesco.aqueduct.pipe.api;

import java.util.List;

public interface LocationService {
    List<String> getClusterUuids(String locationId);
}
