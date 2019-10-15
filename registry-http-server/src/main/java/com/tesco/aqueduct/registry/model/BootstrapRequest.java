package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;

    public void save(TillStorage tillStorage) throws SQLException {
        for (String tillHost : tillHosts) {
            tillStorage.save(
                new Till(tillHost, new Bootstrap(bootstrapType, LocalDateTime.now()))
            );
        }
    }
}
