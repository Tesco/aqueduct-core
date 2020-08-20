package com.tesco.aqueduct.pipe.repository;

import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.pipe.codec.CodecType;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 * In memory repository of messages with optional patching on write and optional compression.
 *
 * The {@link DiffingMessageRepository#put(BinaryMessageWithPatch)} MUST be called from single thread!
 * It using hashmap that is not thread safe.
 * Other methods are thread safe, hence this class is thread safe to read.
 *
 * Reasonable memory efficiency is achieved by storing messages with compressed byte arrays and decompressing them at read time.
 * Appending, finding and scanning efficiency is achieved by using a SkipList as a map, to map offsets to messages.
 * Patching is relatively good as we track previous offset in a hashmap.
 * This approach would also support deletions if needed.
 *
 * - can compress data in memory
 * - supports different codecs (only gzip at the moment)
 * - automatically decompress data on read
 * - smart in deciding should it encode or not per message (encoded might be bigger) with a ratio
 * - provide JSON patch where possible and sensible for data
 * - calculate patch on write, saving it only if it is smaller enough (with a ratio)
 * - efficient for writes - theoretically O(log N) practically constant for current use case
 * - efficient for finding indexes (or next available value if offset does not exist) - O(log N)
 * - efficient for full scans - once element is found it is O(1) to get the next one
 * - optimised for reading with patches - patches are generated during write time and stored compressed
 * - relatively good patching time - as we always know what the previous offset of a key was we can quickly find it and generate a patch
 *
 * Tested for 10gb of JSON on:
 *  - AdoptOpenJDK 14.0.2: -Xmx12g -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC
 *  - AdoptOpenJDK 1.8.0_252 - with defaults, heap
 *
 * Writes with patching are slow (10-30s for 100 000 messages of significant size), one can speed them up significantly by:
 * a) inserting small batches of messages (equal to number of CPUs you want to make busy) and generate patch in parallel
 * b) buffering puts, and patching them in parallel
 *
 * In both cases watch out for the same key in current group.
 * Another option is to generate patch on publish time and save both patch and data in central database.
 */
public class DiffingMessageRepository implements MessageRepository {

    private final NavigableMap<Long, BinaryMessageWithPatch> map = new ConcurrentSkipListMap<>();

    /**
     * Stores mapping between given offsets and it's last key so it is easy to apply a patch.
     *
     * It could also store reference do actual data (BinaryMessageWithPatch).
     * It would be faster (skip log n search for offset), but then care have to be taken
     * with compression and not to hold to too much data.
     */
    private final Map<String, Long> lastKeyOffset = new HashMap<>();

    /**
     * Should we try generating json patches.
     */
    private final boolean patch;

    private final BinaryMessageCodec codec;
    private final BinaryMessageDiff diff;

    public DiffingMessageRepository(boolean patch, CodecType encoder) {
        this.patch = patch;
        this.codec = new BinaryMessageCodec(0.9, encoder);
        this.diff = new BinaryMessageDiff(0.8, codec, JsonHelper.MAPPER);
    }

    @Override
    public void put(@Nonnull BinaryMessageWithPatch message) {
        if (patch && message.getPatch() == null) {
            Long previousOffset = lastKeyOffset.get(message.getKey());

            if(previousOffset != null) {
                BinaryMessageWithPatch encodedPreviousMessage = map.get(previousOffset);
                if (encodedPreviousMessage == null) {
                    throw new IllegalStateException("If we have previous offset we should have also previous message for message" + message.getKey());
                }

                message = diff.generatePatch(encodedPreviousMessage, message);
            }
        }

        // easy to mess up, might move encoding to separate class
        map.put(message.getOffset(), codec.encode(message));

        lastKeyOffset.put(message.getKey(),message.getOffset());
    }

    @Override
    public BinaryMessageWithPatch get(@Nonnull Long offset) {
        return codec.decode(map.get(offset));
    }

    @Override
    public Stream<? extends BinaryMessageWithPatch> streamFrom(@Nonnull Long offset) {
        return map.tailMap(offset).values().stream().map(codec::decode);
    }
}
