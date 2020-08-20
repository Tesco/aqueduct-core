package com.tesco.aqueduct.pipe.repository;


import com.tesco.aqueduct.pipe.codec.CodecType;
import lombok.Value;
import lombok.With;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;

@Value
public class BinaryMessageWithPatch {
    String type;
    String key;
    @Nullable String contentType;
    Long offset;
    ZonedDateTime created;

    /**
     * Data that might be in compressed format.
     */
    @With @Nullable byte[] data;

    /**
     * Compression used on data;
     */
    @With @Nullable
    CodecType dataCodecType;

    /**
     * Original size of the data
     */
    Integer size;

    /**
     * Patch is optional, it might not be available either because we have nothing to patch from,
     * or it is bigger than the data itself.
     */
    @With @Nullable
    BinaryPatch patch;
}

