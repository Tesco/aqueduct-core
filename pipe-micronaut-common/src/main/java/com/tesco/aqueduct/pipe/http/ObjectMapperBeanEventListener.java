package com.tesco.aqueduct.pipe.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;

import javax.inject.Singleton;

@Singleton
public class ObjectMapperBeanEventListener implements BeanCreatedEventListener<ObjectMapper> {
    @Override
    public ObjectMapper onCreated(final BeanCreatedEvent<ObjectMapper> event) {
        final ObjectMapper mapper = event.getBean();
        return JsonHelper.configureObjectMapper(mapper);
    }
}