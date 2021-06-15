package com.tesco.aqueduct.pipe.logger;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.OffsetEntity;
import com.tesco.aqueduct.pipe.api.PipeState;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class PipeLogger {

    private final Logger log;

    private final Map<String, String> fields;

    public PipeLogger(final Logger logger) {
        this.log = logger;
        this.fields = new HashMap<>();
    }

    private PipeLogger(final PipeLogger logger, final Map<String, String> logFields) {
        this.log = logger.log;

        this.fields = new HashMap<>();
        this.fields.putAll(logger.fields);
        this.fields.putAll(logFields);
    }

    public PipeLogger withMessage(final Message message) {
        final Map<String, String> fields = new HashMap<>();
        fields.put("type", message.getType());
        fields.put("key", message.getKey());
        fields.put("contentType", message.getContentType());
        fields.put("offset", String.valueOf(message.getOffset()));
        return new PipeLogger(this, fields);
    }

    public PipeLogger withTypes(final List<String> types) {
        if (!log.isDebugEnabled() || types == null) {
            return this;
        }

        final Map<String, String> fields = Collections.singletonMap(
            "types",
            Arrays.toString(types.toArray())
        );
        return new PipeLogger(this, fields);
    }

    public PipeLogger withOffset(final long offset) {
        final Map<String, String> fields = Collections.singletonMap(
            "offset",
            String.valueOf(offset)
        );
        return new PipeLogger(this, fields);
    }

    public PipeLogger withLocation(final String location) {
        final Map<String, String> fields = Collections.singletonMap(
            "location",
            location
        );
        return new PipeLogger(this, fields);
    }

    public void error(final String where, final String what, final String why) {
        log(where, what, why, log::error);
    }

    public void error(final String where, final String what, final Throwable why) {
        log(where, what, why, log::error);
    }

    public void info(final String where, final String what) {
        if (log.isInfoEnabled()) {
            log(where, what, log::info);
        }
    }

    public void info(final String where, final String what, final Throwable why) {
        if (log.isInfoEnabled()) {
            log(where, what, why, log::info);
        }
    }

    public void debug(final String where, final String what) {
        if (log.isDebugEnabled()) {
            log(where, what, log::debug);
        }
    }

    public void debug(final String where, final String what, final Throwable why) {
        if (log.isDebugEnabled()) {
            log(where, what, why, log::debug);
        }
    }

    private void log(final String where, final String what, final Consumer<String> loggerFunc) {
        log(where, () -> loggerFunc.accept(what));
    }

    private void log(final String where, final String what, final Object why, final BiConsumer<String, Object> loggerFunc) {
        log(where, () -> loggerFunc.accept(what, why));
    }

    private void log(String where, Runnable logger) {
        final Map<String, String> previousContextMap = MDC.getCopyOfContextMap();
        try {
            if (previousContextMap != null) {
                fields.putAll(previousContextMap);
            }
            fields.put("method", where);
            MDC.setContextMap(fields);
            logger.run();
        } finally {
            // recover MDC state as before
            if (previousContextMap != null) {
                MDC.setContextMap(previousContextMap);
            } else {
                MDC.clear();
            }
        }
    }

    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    public PipeLogger withOffset(OffsetEntity offset) {
        final Map<String, String> fields = new HashMap<>();
        fields.put("name", offset.getName().toString());
        fields.put("value", offset.getValue().toString());
        return new PipeLogger(this, fields);
    }

    public PipeLogger withPipeState(PipeState pipeState) {
        final Map<String, String> fields = new HashMap<>();
        fields.put("name", "pipeState");
        fields.put("value", pipeState.toString());
        return new PipeLogger(this, fields);
    }
}