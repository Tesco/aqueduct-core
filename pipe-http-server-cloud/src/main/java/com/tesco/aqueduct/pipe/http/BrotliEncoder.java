package com.tesco.aqueduct.pipe.http;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.LoggerFactory;

public class BrotliEncoder extends MessageToByteEncoder<ByteBuf> {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(BrotliEncoder.class));

    public BrotliEncoder() {
        final boolean isBrotliAvailable = BrotliLoader.isBrotliAvailable();
        LOG.info("encode", "Load Brotli: " + isBrotliAvailable);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
        final ByteBufInputStream byteBufInputStream = new ByteBufInputStream(msg);

        Encoder.Parameters params = new Encoder.Parameters().setQuality(4);

        long startTime = System.currentTimeMillis();
        long startTimeNano = System.nanoTime();

        try (BrotliOutputStream brotliOutputStream = new BrotliOutputStream(new ByteBufOutputStream(out), params)) {
            brotliOutputStream.write(msg.array());
        }

        long endTime = System.currentTimeMillis();
        long endTimeNano = System.nanoTime();

        LOG.info("encode", "Time taken to encode in milliseconds: " + (endTime - startTime));
        LOG.info("encode", "Time taken to encode in nanoseconds: " + (endTimeNano - startTimeNano));
        LOG.info("encode", "UnCompressed size: " + msg.array().length);
        LOG.info("encode", "Compressed size: " + out.readableBytes());
        LOG.info("encode", "Compression ratio: " +
            ((double)msg.array().length)/((double)out.readableBytes()));

    }
}
