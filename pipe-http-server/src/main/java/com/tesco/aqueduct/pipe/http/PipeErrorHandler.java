package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.http.hateoas.Link;
import org.slf4j.LoggerFactory;

@Controller
public class PipeErrorHandler {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeErrorHandler.class));

    @Error(global = true, status = HttpStatus.METHOD_NOT_ALLOWED)
    public MutableHttpResponse<JsonError> handleMethodNotAllowed(final HttpRequest<?> request){
        // Following micronaut standard for now. It is different than Tesco standard
        final JsonError error = new JsonError("Page Not Found");
        error.link(Link.SELF, Link.of(request.getUri()));

        LOG.error("pipe error handler", "method not allowed", error.getMessage());
        return HttpResponse.notFound(error);
    }
}
