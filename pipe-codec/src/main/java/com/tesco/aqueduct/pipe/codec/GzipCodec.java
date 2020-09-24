package com.tesco.aqueduct.pipe.codec;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.context.annotation.Value;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Singleton
public class GzipCodec implements Codec {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(GzipCodec.class));

    private final int level;
    private final boolean logging;

    /**
     * Allow to set compression level. Differences usually are not worth the effort.
     * But it has not been tested yet on real data.
     *
     * @param level Compression level as defined by constants in {@link Deflater}
     */
    public GzipCodec(
        @Value("${http.codec.gzip.level:-1}") int level,
        @Value("${compression.logging:false}") boolean logging
    ){
        this.level = level;
        this.logging = logging;
    }

    @Override
    public String getHeaderType() {
        return "gzip";
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
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream) {{
            def.setLevel(level);
        }}) {
            gzipOutputStream.write(input);
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
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(input));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int b;
            while ((b = in.read()) != -1) {
                out.write(b);
            }
            return out.toByteArray();
        } catch (IOException ioException) {
            LOG.error("Codec", "Error encoding content", ioException);
            throw new PipeCodecException("Error encoding content", ioException);
        }
    }
}
