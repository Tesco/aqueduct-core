package com.tesco.aqueduct.pipe.storage.sqlite;

import lombok.Value;

@Value
public class OffsetConsistency {
    long sum;
    long offset;
}
