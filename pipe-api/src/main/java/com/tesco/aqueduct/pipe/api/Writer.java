package com.tesco.aqueduct.pipe.api;

public interface Writer {

    /**
     * Put messages ordered by offset to a store.
     * Any missing offset should never arrive later.
     */
    @Deprecated
    default void write(Iterable<Message> messages) {
        messages.forEach(this::write);
    }

    void write(PipeEntity pipeEntity);

    @Deprecated
    void write(Message message);

    @Deprecated
    void write(OffsetEntity offset);

    @Deprecated
    void write(PipeState pipeState);

    void deleteAll();
}
