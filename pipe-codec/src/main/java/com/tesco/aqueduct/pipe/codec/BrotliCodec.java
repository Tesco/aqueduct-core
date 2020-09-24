package com.tesco.aqueduct.pipe.codec;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.dec.BrotliInputStream;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.context.annotation.Value;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Singleton
public class BrotliCodec implements Codec {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(BrotliCodec.class));

    private final boolean logging;

    /**
     * Allow to set compression level. It has not been tested yet on real data.
     *
     * @param qualityLevel Compression level
     */
    public BrotliCodec(
        @Value("${http.codec.brotli.level:4}") int qualityLevel,
        @Value("${compression.logging:false}") boolean logging
    ) {
        loadBrotli();
        Encoder.Parameters parameters = new Encoder.Parameters();
        parameters.setQuality(qualityLevel);
        this.logging = logging;
    }

    private void loadBrotli() {
        LOG.info("Codec", "Load Brotli: " + BrotliLoader.isBrotliAvailable());
    }

    @Override
    public byte[] encode(byte[] input) {
        if (input == null) {
            return null;
        }
        if (logging) {
            LOG.info("pre-encode:size", String.valueOf(input.length));
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BrotliOutputStream brotliOutputStream =
             new BrotliOutputStream(outputStream, new Encoder.Parameters().setQuality(4))
        ) {
            brotliOutputStream.write(input);
        } catch (IOException ioException) {
            LOG.error("Codec", "Error encoding content", ioException);
            throw new PipeCodecException("Error encoding content", ioException);
        }
        final byte[] encodedBytes = outputStream.toByteArray();
        if (logging) {
            LOG.info("post-encode:size", String.valueOf(encodedBytes.length));
        }
        return encodedBytes;
    }

    @Override
    public byte[] decode(byte[] input) {
        if (input == null) {
            return null;
        }
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final BrotliInputStream brotliInputStream = new BrotliInputStream(new ByteArrayInputStream(input))) {

            int decodedByte=brotliInputStream.read();
            while (decodedByte != -1) {
                byteArrayOutputStream.write(decodedByte);
                decodedByte = brotliInputStream.read();
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException ioException) {
            LOG.error("Codec", "Error decoding bytes.", ioException);
            throw new PipeCodecException("Error decoding bytes.", ioException);
        }
    }

    @Override
    public String getHeaderType() {
        return "br";
    }
}

