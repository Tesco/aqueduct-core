package com.tesco.aqueduct.pipe.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.pipe.codec.CodecType;

import java.io.IOException;

public class BinaryMessageDiff {

    private final BinaryMessageCodec codec;

    /**
     * Merging patch on client side require significant IO to get previous version and CPU to apply it.
     * If patch does not give much, it is to costly to have it.
     */
    private final double patchRatioThreshold;
    private final ObjectMapper jsonMapper;


    public BinaryMessageDiff(double patchRatioThreshold, BinaryMessageCodec codec, ObjectMapper jsonMapper) {
        this.codec = codec;
        this.patchRatioThreshold = patchRatioThreshold;
        this.jsonMapper = jsonMapper;
    }

    public BinaryMessageWithPatch generatePatch(BinaryMessageWithPatch encodedSource, BinaryMessageWithPatch potentiallyEncodedTarget) {
        if(
            "application/json".equals(encodedSource.getContentType()) &&
            "application/json".equals(potentiallyEncodedTarget.getContentType()) &&
            !"runscope-test-type".equals(encodedSource.getType()) && //FIXME: why oh why runscope send invalid json as application/json!!!
            encodedSource.getData() != null &&
            potentiallyEncodedTarget.getData() != null
        ) {
            byte[] sourceData = codec.decode(encodedSource).getData();
            BinaryMessageWithPatch plainTarget = codec.decode(potentiallyEncodedTarget);
            byte[] targetData = plainTarget.getData();

            try {
                //
                JsonPatch jsonPatch = JsonDiff.asJsonPatch(
                    jsonMapper.readTree(sourceData),
                    jsonMapper.readTree(targetData)
                );
                byte[] patchBytes = JsonHelper.MAPPER.writeValueAsBytes(jsonPatch);

                if(patchBytes.length < patchRatioThreshold * targetData.length) {
                    return potentiallyEncodedTarget.withPatch(
                            new BinaryPatch(
                                encodedSource.getOffset(),
                                patchBytes,
                                CodecType.NONE,
                                patchBytes.length
                            )
                    );
                }
            } catch (IOException e) {
                // TODO: log failure
                System.out.println("PANIC! " + e.getMessage());
                System.out.println(new String(sourceData));
                System.out.println(new String(targetData));
            }
        }

        // do not patch
        return potentiallyEncodedTarget;
    }
}
