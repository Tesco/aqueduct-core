package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.registry.PipeLoadBalancer
import com.tesco.aqueduct.registry.SelfRegistrationTask
import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime

class InternalHttpPipeClientIntegrationSpec extends Specification {

    @Shared @AutoCleanup ErsatzServer server
    @Shared @AutoCleanup("stop") ApplicationContext context

    PipeLoadBalancer loadBalancer
    InternalHttpPipeClient client

    def setupSpec() {
        server = new ErsatzServer()
        server.start()

        context = ApplicationContext
            .build()
            .properties(
                "pipe.http.latest-offset.attempts": 1,
                "pipe.http.latest-offset.delay": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "pipe.tags": [:]
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .start()
    }

    def setup() {
        client = context.getBean(InternalHttpPipeClient)

        loadBalancer = context.getBean(PipeLoadBalancer)
        loadBalancer.update([new URL(server.getHttpUrl())])
    }

    def cleanup() {
        server.clearExpectations()
    }

    @Unroll
    def "Client is calling correct link with proper parameters"() {
        given:
        server.expectations {
            get("/pipe/$offset") {
                header('Accept', 'application/json')
                header('Accept-Encoding', 'gzip, deflate')
                queries(store: store)
                called(1)

                responder {
                    header("Retry-After", "1")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "$type",
                            "key": "x",
                            "contentType": "$ct",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "tags": { "example":"value"}, 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        when:
        def messages = client.httpRead([store: [store]], offset).body()

        def expectedMessage = new Message(
            type,
            "x",
            ct,
            offset,
            ZonedDateTime.parse("2018-10-01T13:45:00Z"),
            [example: ["value"]],
            '{ "valid": "json" }'
        )
        then:
        server.verify()
        messages[0] == expectedMessage

        where:
        type    | ct             | offset | store
        "type1" | "contentType1" | 123    | "6735"
        "type2" | "contentType2" | 123    | "6735"
        "type1" | "contentType3" | 111    | "6735"
        "type2" | "contentType4" | 123    | "4896"
    }

    def "can get latest offset"(){
        given:
        server.expectations {
            get("/pipe/offset/latest") {
                called(1)

                responder {
                    contentType('application/json')
                    body('123')
                }
            }
        }

        when:
        def latest = client.getLatestOffsetMatching([:])
        then:
        server.verify()
        latest == 123
    }

    def "can get latest offset with tags"(){
        given:
        server.expectations {
            get("/pipe/offset/latest") {
                called(1)
                query("tag","a")

                responder {
                    contentType('application/json')
                    body('100')
                }
            }
        }

        when:
        def latest = client.getLatestOffsetMatching([tag:["a"]])

        then:
        server.verify()
        latest == 100
    }
}
