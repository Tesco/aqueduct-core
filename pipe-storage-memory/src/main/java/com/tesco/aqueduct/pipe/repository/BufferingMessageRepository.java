package com.tesco.aqueduct.pipe.repository;

import com.tesco.aqueduct.pipe.codec.CodecType;

public class BufferingMessageRepository extends DiffingMessageRepository {
    public BufferingMessageRepository(boolean patch, CodecType codecType) {
        super(patch, codecType);
    }
}
