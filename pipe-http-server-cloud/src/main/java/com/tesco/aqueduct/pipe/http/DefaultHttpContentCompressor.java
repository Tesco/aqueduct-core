package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.server.netty.SmartHttpContentCompressor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpResponse;

@ChannelHandler.Sharable
@Replaces(SmartHttpContentCompressor.class)
public class DefaultHttpContentCompressor extends HttpContentCompressor {

    private final int compressionLevel;
    private final int windowBits;
    private final int memLevel;
    private final int contentSizeThreshold;
    private ChannelHandlerContext ctx;

    public DefaultHttpContentCompressor() {
        this(9);
    }

    public DefaultHttpContentCompressor(int compressionLevel) {
        this(compressionLevel, 15, 8, 0);
    }

    public DefaultHttpContentCompressor(int compressionLevel, int windowBits, int memLevel) {
        this(compressionLevel, windowBits, memLevel, 0);
    }

    public DefaultHttpContentCompressor(int compressionLevel, int windowBits, int memLevel, int contentSizeThreshold) {
        super(compressionLevel, windowBits, memLevel, contentSizeThreshold);
        this.compressionLevel=compressionLevel;
        this.windowBits=windowBits;
        this.memLevel=memLevel;
        this.contentSizeThreshold=contentSizeThreshold;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    protected Result beginEncode(HttpResponse httpResponse, String acceptEncoding) throws Exception {
/*
        if (this.contentSizeThreshold > 0) {
            if (httpResponse instanceof HttpContent &&
                    ((HttpContent) httpResponse).content().readableBytes() < contentSizeThreshold) {
                return null;
            }
        }

        String contentEncoding = httpResponse.headers().get(HttpHeaderNames.CONTENT_ENCODING);
        if (contentEncoding != null) {
            // Content-Encoding was set, either as something specific or as the IDENTITY encoding
            // Therefore, we should NOT encode here
            return null;
        }

        ZlibWrapper wrapper = determineWrapper(acceptEncoding);
        if (wrapper == null) {
            return null;
        }

        String targetContentEncoding;
        switch (wrapper) {
            case GZIP:
                targetContentEncoding = "gzip";
                break;
            case ZLIB:
                targetContentEncoding = "deflate";
                break;
            default:
                throw new Error();
        }

        final ZlibEncoder zlibEncoder = new CustomJdkZlibEncoder(wrapper, compressionLevel, windowBits, memLevel);
*/

        return new Result(
            "brotli",
            new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                ctx.channel().config(), new BrotliEncoder()));
    }
}
