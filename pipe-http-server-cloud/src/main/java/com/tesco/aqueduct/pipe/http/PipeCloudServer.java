package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.registry.http.NodeRegistryControllerV1;
import com.tesco.aqueduct.registry.http.NodeRegistryControllerV2;
import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import org.slf4j.LoggerFactory;

@OpenAPIDefinition(
        info = @Info(
                title = "Aqueduct",
                version = "1.0",
                description = "Aqueduct Cloud API",
                license = @License(name = "MIT", url = "https://opensource.org/licenses/MIT")
        )
)
public class PipeCloudServer {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeCloudServer.class));

    public static void main(final String[] args) {

        LOG.info("Pipe cloud server", "server started");

        Micronaut.run(new Class[]{
            PipeReadController.class,
            PipeErrorHandler.class,
            NodeRegistryControllerV1.class,
            NodeRegistryControllerV2.class
        });
    }
}
