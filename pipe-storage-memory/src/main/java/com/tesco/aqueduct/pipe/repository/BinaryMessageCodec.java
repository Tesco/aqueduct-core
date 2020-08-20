package com.tesco.aqueduct.pipe.repository;

import com.tesco.aqueduct.pipe.codec.*;

public class BinaryMessageCodec {

    /**
     * There is significant CPU cost for compression of data.
     * If compression does not give much, there is no point to waste CPU
     */
    private final double compressionRationThreshold;
    private final CodecType defaultCodec;

    private final Codec gzip = new GzipCodec();
    private final Codec brotli = new BrotliCodec();

    public BinaryMessageCodec(double compressionRationThreshold, CodecType defaultCodec){
        this.compressionRationThreshold = compressionRationThreshold;
        this.defaultCodec = defaultCodec;
    }

    protected Codec getCodec(CodecType codecType) {
        switch (codecType) {
            case GZIP:
                return gzip;
            case BROTLI:
                return brotli;
            case NONE:
                return null;
        }

        throw new IllegalStateException("Using unsupported codec");
    }

    public BinaryMessageWithPatch encode(BinaryMessageWithPatch message) {
        if(defaultCodec == CodecType.NONE) {
            return message;
        }
        Codec codec = getCodec(defaultCodec);

        // encode data
        byte[] data = message.getData();
        CodecType codecType = message.getDataCodecType();
        if(
            message.getData() != null &&
            message.getDataCodecType() == CodecType.NONE
        ) {
            byte[] encoded = codec.encode(message.getData());

            if(encoded.length < compressionRationThreshold * message.getData().length) {
                data = encoded;
                codecType = codec.getType();
            }
        }

        // encode patch
        BinaryPatch patch = message.getPatch();
        if(
            patch != null &&
            patch.getCodecType() == CodecType.NONE
        ) {
            byte[] encoded = codec.encode(patch.getPatch());

            if(encoded.length < compressionRationThreshold * patch.getPatch().length) {
                patch = message.getPatch()
                    .withPatch(encoded)
                    .withCodecType(codec.getType())
                ;
            }
        }

        // intern strings to save some extra space, this can reach gigabytes for tens of millions of messages
        return new BinaryMessageWithPatch(
            message.getType().intern(), // types repeat a lot, worth interning
            message.getKey(), // TODO: intern? have not measured difference for that field
            message.getContentType().intern(), // content types repeat a lot worth interning
            message.getOffset(),
            message.getCreated(),
            data,
            codecType,
            message.getSize(),
            patch
        );
    }


    public BinaryMessageWithPatch decode(BinaryMessageWithPatch message) {
        // decode data
        if(
            message.getDataCodecType() != null &&
            message.getDataCodecType() != CodecType.NONE
        ) {
            message = message
                .withData(getCodec(message.getDataCodecType()).decode(message.getData()))
                .withDataCodecType(CodecType.NONE)
            ;
        }

        // decode patch
        if(
            message.getPatch() != null &&
            message.getPatch().getCodecType() != null &&
            message.getPatch().getCodecType() != CodecType.NONE
        ) {
            message = message.withPatch(
                message.getPatch()
                    .withPatch(
                        getCodec(message.getPatch().getCodecType())
                            .decode(message.getPatch().getPatch())
                    )
                    .withCodecType(CodecType.NONE)
            );
        }

        return message;
    }
}
