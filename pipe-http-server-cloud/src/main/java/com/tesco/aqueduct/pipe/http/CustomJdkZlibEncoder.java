package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import org.slf4j.LoggerFactory;

public class CustomJdkZlibEncoder extends JdkZlibEncoder {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(CustomJdkZlibEncoder.class));
    private int compressionLevel;

    public CustomJdkZlibEncoder(ZlibWrapper wrapper, int compressionLevel, int windowBits, int memLevel) {
        super(wrapper, compressionLevel);
        this.compressionLevel = compressionLevel;
    }

    @Override
    public boolean isClosed() {
        return super.isClosed();
    }

    @Override
    public ChannelFuture close() {
        return super.close();
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return super.close(promise);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
        LOG.info("encode", "Compression level: " + compressionLevel);

        long startTime = System.currentTimeMillis();
        long startTimeNano = System.nanoTime();
        super.encode(ctx, msg, out);
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
