package com.tesco.aqueduct.registry

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.registry.model.Node
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.inject.qualifiers.Qualifiers
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZonedDateTime
import java.util.function.Supplier

class RegistryClientIntegrationSpec extends Specification {

    private static final URL MY_HOST = new URL("http://localhost")
    String host1 = "http://host1"
    String host2 = "http://host2"

    @Shared @AutoCleanup ErsatzServer server

    Supplier<Node> selfSummarySupplier = Mock()
    Supplier<Map<String, Object>> providerMetricsSupplier = Mock()
    def setupSpec() {
        server = new ErsatzServer()
        server.start()
    }

    def 'test registry endpoint'() {
        given: "A dummy server's application context"

        def context = ApplicationContext
            .build()
            .properties(
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl() + "/v2",
                "pipe.http.client.healthcheck.interval": "1m",
                "pipe.http.register.retry.interval": "1s",
                "pipe.http.registration.interval": "1m"
            )
            .build()
            .registerSingleton(Supplier.class, selfSummarySupplier, Qualifiers.byName("selfSummarySupplier"))
            .registerSingleton(Supplier.class, providerMetricsSupplier, Qualifiers.byName("providerMetricsSupplier"))
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(server.getHttpUrl())),
                File.createTempFile("provider", "properties")
            ))
            .start()

        and: "a fake response from the server"
        server.expectations {
            post("/v2/registry") {
                header("Accept-Encoding", "gzip, deflate")
                called(1)

                responder {
                    contentType("application/json")
                    body(""" [ "$host1", "$host2" ]""")
                }
            }
            get("/pipe/_status") {
                called(1)
                responder {
                    contentType("application/json")
                }
            }
        }
        and: "A Micronaut-generated Client"
        def client = context.getBean(RegistryClient)

        and: "We have a node in the NodeRegistry"

        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status("initialising")
            .lastSeen(ZonedDateTime.now())
            .build()

        selfSummarySupplier.get() >> {
            myNode
        }

        when: "We call register using the Micronaut client"
        def response = client.register(myNode)

        then: "We expect the dummy server to return a list of URLs"
        response.size() == 2

        response == [new URL(host1), new URL(host2)]

        cleanup:
        server.stop()
    }
}

