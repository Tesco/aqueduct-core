package com.tesco.aqueduct.registry;

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

    public RegistryLogger(Logger logger) {
        this.log = logger;
        this.fields = new HashMap<>();
    }

    private RegistryLogger(RegistryLogger logger, Map<String, String> logFields) {
        this.log = logger.log;

        this.fields = new HashMap<>();
        this.fields.putAll(logger.fields);
        this.fields.putAll(logFields);
    }

    public RegistryLogger withNode(Node node) {

        Map<String, String> fields = new HashMap<>();

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

    public void error(String where, String what, String why) {
        log(where, what, why, log::error);
    }

    public void error(String where, String what, Throwable why) {
        log(where, what, why, log::error);
    }

    public void info(String where, String what) {
        if (log.isInfoEnabled()) {
            log(where, what, log::info);
        }
    }

    public void info(String where, String what, Throwable why) {
        if (log.isInfoEnabled()) {
            log(where, what, why, log::info);
        }
    }

    public void debug(String where, String what) {
        if (log.isDebugEnabled()) {
            log(where, what, log::debug);
        }
    }

    public void debug(String where, String what, Throwable why) {
        if (log.isDebugEnabled()) {
            log(where, what, why, log::debug);
        }
    }

    private void log(String where, String what, Consumer<String> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what);
        } finally {
            MDC.clear();
        }
    }

    private void log(String where, String what, Object why, BiConsumer<String, Object> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what, why);
        } finally {
            MDC.clear();
        }
    }
}
