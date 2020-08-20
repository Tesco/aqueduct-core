package com.tesco.aqueduct.pipe.codec;

import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec implements Codec {

    private int level;

    /**
     * Allow to set compression level. Differences usually are not worth the effort.
     * But it has not been tested yet on real data.
     *
     * @param level Compression level as defined by constants in {@link Deflater}
     */
    public GzipCodec(int level){
        this.level = level;
    }

    public GzipCodec(){
        this.level = Deflater.DEFAULT_COMPRESSION;
    }

    @Override
    public CodecType getType() {
        return CodecType.GZIP;
    }

    @SneakyThrows
    @Override
    public byte[] encode(byte[] input) {
        if(input == null) {
            return null;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream) {{
            def.setLevel(level);
        }};
        gzipOutputStream.write(input);
        gzipOutputStream.close();
        return outputStream.toByteArray();
    }

    @SneakyThrows
    @Override
    public byte[] decode(byte[] input) {
        if(input == null) {
            return null;
        }

        GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(input));
        return getBytes(inputStream);
    }
}
