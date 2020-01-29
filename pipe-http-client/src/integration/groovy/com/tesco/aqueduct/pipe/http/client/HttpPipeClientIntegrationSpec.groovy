package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.PipeState
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.client.PipeServiceInstance
import com.tesco.aqueduct.registry.client.SelfRegistrationTask
import com.tesco.aqueduct.registry.client.ServiceList
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class HttpPipeClientIntegrationSpec extends Specification {

    @Shared @AutoCleanup ErsatzServer server
    @Shared @AutoCleanup("stop") ApplicationContext context

    HttpPipeClient client

    def setupSpec() {
        server = new ErsatzServer()
        server.start()
    }

    def setup() {
        context = ApplicationContext
            .build()
            .properties(
                "pipe.http.latest-offset.attempts": 1,
                "pipe.http.latest-offset.delay": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl() + "/v2",
                "micronaut.caches.health-check.maximum-size": 20,
                "micronaut.caches.health-check.expire-after-write": "5s"
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .registerSingleton(Mock(TokenProvider))
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(server.getHttpUrl())),
                File.createTempFile("provider", "properties")
            ))
            .start()

        client = context.getBean(HttpPipeClient)
    }

    def cleanup() {
        server.clearExpectations()
    }

    def "can get pipe state, and is cached successfully"(){
        given:
        server.expectations {
            get("/pipe/state") {
                called(1)
                query("type", "a")

                responder {
                    contentType('application/json')
                    body('{"upToDate":true,"localOffset":"1000"}')
                }
            }
        }

        when: "I call state once"
        def state = client.getPipeState(["a"])

        and: "I call state again"
        def state2 = client.getPipeState(["a"])

        then: "the first result was 'up to date'"
        state == new PipeStateResponse(PipeState.UP_TO_DATE, 1000)

        and: "I got the same result from the second call"
        state2 == state

        and: "the server has only been called once (the result was cached)"
        server.verify()
    }

    def "can get pipe state and failed state is not cached"(){
        given:
        server.expectations {
            get("/pipe/state") {
                called(2)
                query("type", "a")

                responder {
                    contentType('application/json')
                    body('{"upToDate":false,"localOffset":"1000"}')
                }
            }
        }

        when: "I call state once"
        def state = client.getPipeState(["a"])

        and: "I call state again"
        def state2 = client.getPipeState(["a"])

        then: "the first result was 'up to date'"
        state == new PipeStateResponse(PipeState.OUT_OF_DATE, 1000)

        and: "I got the same result from the second call"
        state2 == state

        and: "the server has only been called once (the result was cached)"
        server.verify()
    }
}
