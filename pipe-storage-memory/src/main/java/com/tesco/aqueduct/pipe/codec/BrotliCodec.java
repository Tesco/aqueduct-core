package com.tesco.aqueduct.pipe.codec;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.dec.BrotliInputStream;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class BrotliCodec implements Codec {

    public BrotliCodec() {
        BrotliLoader.isBrotliAvailable();
    }

    @Override
    public CodecType getType() {
        return CodecType.BROTLI;
    }

    @SneakyThrows
    @Override
    public byte[] encode(byte[] input) {
        if(input == null) {
            return null;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder.Parameters params = new Encoder.Parameters().setQuality(4);

        BrotliOutputStream brotliOutputStream = new BrotliOutputStream(outputStream, params);
        brotliOutputStream.write(input);
        brotliOutputStream.close();

        return outputStream.toByteArray();
    }

    @SneakyThrows
    @Override
    public byte[] decode(byte[] input) {
        if(input == null) {
            return null;
        }

        BrotliInputStream inputStream = new BrotliInputStream(new ByteArrayInputStream(input));

        return getBytes(inputStream);
    }
}
