package com.tesco.aqueduct.pipe.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public interface Codec {
    CodecType getType();
    byte[] encode(byte[] input);
    byte[] decode(byte[] input);

    default byte[] getBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int b;

        while ((b = inputStream.read()) != -1) {
            out.write(b);
        }

        return out.toByteArray();
    }
}
