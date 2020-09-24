package com.tesco.aqueduct.pipe.codec;

public interface Codec {
    String getHeaderType();

    byte[] encode(byte[] input);

    byte[] decode(byte[] input);
}
