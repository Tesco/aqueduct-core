package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.OptionalLong;

public interface Reader {
    MessageResults read(List<String> types, long offset, List<String> targetUuids);
    OptionalLong getOffset(OffsetName offsetName);
    PipeState getPipeState();
    default long getOffsetConsistencySum(long offset, List<String> targetUuids) {
      return 0; //null op
    };
    default void runVisibilityCheck() {
        //null op
    };
}
