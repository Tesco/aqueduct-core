package com.tesco.aqueduct.pipe.codec;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.context.annotation.Value;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


@ChannelHandler.Sharable
public class GzipCodec extends MessageToByteEncoder<ByteBuf> implements Codec {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(BrotliCodec.class));

    private int level;

    /**
     * Allow to set compression level. Differences usually are not worth the effort.
     * But it has not been tested yet on real data.
     *
     * @param level Compression level as defined by constants in {@link Deflater}
     */
    public GzipCodec(@Value("${http.codec.gzip.level:-1}") int level){
        this.level = level;
    }

    public GzipCodec() {
        this.level = Deflater.DEFAULT_COMPRESSION;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
        out.writeBytes(encode(msg.array()));
    }

    @Override
    public CodecType getType() {
        return CodecType.GZIP;
    }

    @Override
    public byte[] encode(byte[] input) {
        if (input == null) {
            return null;
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
        return outputStream.toByteArray();
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