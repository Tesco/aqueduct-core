package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.repository.BinaryMessageWithPatch;
import com.tesco.aqueduct.pipe.codec.CodecType;
import com.tesco.aqueduct.pipe.repository.MessageRepository;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Example storage based on repository to do filtering and response size limiting.
 * If we want to limit by count instead of size, it is better to do that in repository,
 * as it might be doing complex operation to pick every next message - uncompressing and/or diffing.
 */
public class RepositoryBasedInMemoryStorage implements DistributedStorage {

    private final MessageRepository repository;
    private final int fetchCountLimit;
    private PipeState state = PipeState.UNKNOWN;

    public RepositoryBasedInMemoryStorage(int fetchLimit, MessageRepository repository) {
        this.repository = repository;
        this.fetchCountLimit = fetchLimit;
    }

    protected BinaryMessageWithPatch encode(Message message) {
        return new BinaryMessageWithPatch(
            message.getType().intern(),
            message.getKey(), //TODO: there might be some small gain from interning the key
            message.getContentType().intern(),
            message.getOffset(),
            message.getCreated(),
            message.getData() == null ? null : message.getData().getBytes(),
            CodecType.NONE,
            message.getSize() == null ? null : message.getSize().intValue(),
            null
        );
    }

    protected Message decode(BinaryMessageWithPatch message) {
        return new Message(
            message.getType(),
            message.getKey(),
            message.getContentType(),
            message.getOffset(),
            message.getCreated(),
            message.getData() == null ? null : new String(message.getData()),
            message.getSize() == null ? null : message.getSize().longValue()
        );
    }

    @Override
    public MessageResults read(List<String> types, long offset, List<String> targetUuids) {
        Stream<? extends BinaryMessageWithPatch> messageStream = repository.streamFrom(offset);

        if(types != null && !types.isEmpty()) {
            messageStream = messageStream.filter(m -> types.contains(m.getType()));
        }

        List<Message> messages = messageStream
            .map(this::decode)
            .limit(fetchCountLimit)
            .collect(toList());

        return new MessageResults(messages, 0, getGlobalOffset(), getPipeState());
    }

    private OptionalLong getGlobalOffset() {
        // TODO: +/- 1? or controlled externally? is that even part of this api?
        return OptionalLong.empty();
    }

    // bad design, those are 3 different methods some of which does not belong to some interfaces local does not belong to dist
    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        throw new NotImplementedException();
    }

    @Override
    public PipeState getPipeState() {
        return state;
    }

    @Override
    public void write(Message message) {
        repository.put(encode(message));
    }

    @Override
    public void write(OffsetEntity offset) {
        throw new NotImplementedException();
    }

    //TODO: lack of symmetry: either that one should be a setter or the other should be "read"
    @Override
    public void write(PipeState pipeState) {
        state = pipeState;
    }

    @Override
    public void deleteAll() {
        throw new NotImplementedException();
    }
}
