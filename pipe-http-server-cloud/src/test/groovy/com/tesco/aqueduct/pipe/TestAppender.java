package com.tesco.aqueduct.pipe;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TestAppender extends AppenderBase<ILoggingEvent> {
    static List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void append(ILoggingEvent e) {
        events.add(e);
    }

    public static List<ILoggingEvent> getEvents() {
        return events;
    }

    public static void clearEvents() {
        events.clear();
    }
}
