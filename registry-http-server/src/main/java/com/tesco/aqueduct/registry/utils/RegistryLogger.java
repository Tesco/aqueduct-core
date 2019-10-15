package com.tesco.aqueduct.registry.utils;

import com.tesco.aqueduct.registry.model.Node;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.net.URL;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RegistryLogger {

    private final Logger log;

    private final Map<String, String> fields;

    public RegistryLogger(final Logger logger) {
        this.log = logger;
        this.fields = new HashMap<>();
    }

    private RegistryLogger(final RegistryLogger logger, final Map<String, String> logFields) {
        this.log = logger.log;

        this.fields = new HashMap<>();
        this.fields.putAll(logger.fields);
        this.fields.putAll(logFields);
    }

    public RegistryLogger withNode(final Node node) {
        final Map<String, String> fields = new HashMap<>();
        fields.put("id", node.getId());
        fields.put("group", node.getGroup());
        fields.put("localUrl", node.getLocalUrl().toString());
        fields.put("offset", String.valueOf(node.getOffset()));
        fields.put("status", node.getStatus());
        fields.put(
                "following",
                Optional.ofNullable(node.getFollowing())
                        .orElseGet(Collections::emptyList)
                        .stream()
                        .map(URL::toString)
                        .collect(Collectors.joining(","))
        );
        fields.put(
                "requestedToFollow",
                Optional.ofNullable(node.getRequestedToFollow())
                        .orElseGet(Collections::emptyList)
                        .stream()
                        .map(URL::toString)
                        .collect(Collectors.joining(","))
        );
        fields.put("lastSeen", String.valueOf(node.getLastSeen()));
        fields.put("providerLastAckOffset", String.valueOf(node.getProviderLastAckOffset()));
        fields.put("providerLastAckTime", String.valueOf(node.getProviderLastAckTime()));

        if(node.getPipe() != null) {
            node.getPipe().entrySet().forEach(e ->
                fields.put("pipe." + e.getKey(), e.getValue())
            );
        }

        if(node.getProvider() != null) {
            node.getProvider().entrySet().forEach(e ->
                fields.put("provider." + e.getKey(), e.getValue())
            );
        }

        return new RegistryLogger(this, fields);
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
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what);
        } finally {
            MDC.clear();
        }
    }

    private void log(final String where, final String what, final Object why, final BiConsumer<String, Object> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what, why);
        } finally {
            MDC.clear();
        }
    }
}
