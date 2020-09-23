package com.tesco.aqueduct.pipe.http.client

import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.pipe.codec.GzipCodec
import com.tesco.aqueduct.registry.client.PipeServiceInstance
import com.tesco.aqueduct.registry.client.SelfRegistrationTask
import com.tesco.aqueduct.registry.client.ServiceList
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.exceptions.HttpClientResponseException
import org.junit.Rule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*

class InternalGzipHttpPipeClientIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    InternalGzipHttpPipeClient client

    def setup() {
        context = ApplicationContext
            .build()
            .properties(
                "pipe.http.client.attempts": 1,
                "pipe.http.client.delay": "500ms",
                "pipe.http.client.reset": "1s",
                "pipe.http.client.url": wireMockRule.baseUrl(),
                "registry.http.client.url": wireMockRule.baseUrl() + "/v2",
                "micronaut.caches.health-check.maximum-size": 20,
                "micronaut.caches.health-check.expire-after-write": "5s"
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .registerSingleton(Mock(TokenProvider))
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl())),
                File.createTempFile("provider", "properties")
            ))
            .start()
        client = context.getBean(InternalGzipHttpPipeClient)
    }

    def "Client is calling correct link with proper parameters with Gzip encoding"() {
        given:
        def responseString = """[
            {
                "type": "type1",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""
        def gzipCodec = new GzipCodec()
        def encodedBytes = gzipCodec.encode(responseString.bytes)

        and:
        stubFor(
            get(urlEqualTo("/pipe/0?type=type1&location=location1"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('gzip'))
            .willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withHeader("X-Content-Encoding", "gzip")
                .withHeader("Content-Encoding", "gzip")
                .withStatus(200)
                .withBody(encodedBytes)
            )
        )
        when:
        byte[] responseBytes = client.httpRead(["type1"], 0, "location1").body()

        then:
        new String(responseBytes) == responseString
    }

    @Unroll
    def "Client is calling correct link with proper parameters"() {
        given:
        def responseString = """[
            {
                "type": "type1",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""
        def gzipCodec = new GzipCodec()
        def encodedBytes = gzipCodec.encode(responseString.bytes)

        and:
        stubFor(
            get(urlEqualTo("/pipe/$offset?type=$type&location=$location"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('gzip'))
                .willReturn(aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withHeader("X-Content-Encoding", "gzip")
                    .withHeader("Content-Encoding", "gzip")
                    .withStatus(200)
                    .withBody(encodedBytes)
                )
        )

        when:
        client.httpRead([type], offset, location).body()

        then:
        verify(
            getRequestedFor(urlEqualTo("/pipe/$offset?type=$type&location=$location"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('gzip'))
        )

        where:
        type    | location    | ct             | offset
        "type1" | "location1" | "contentType1" | 123
        "type2" | "location2" | "contentType2" | 123
        "type1" | "location1" | "contentType3" | 111
        "type2" | "location2" | "contentType4" | 123
    }

    def "Pipe service circuit is opened when it returns 5xx to connect for given number of times"() {
        given: "a pipe service returning 5xx"
        stubFor(
            get(urlEqualTo("/pipe/0?type=someType&location=someLocation"))
                .withHeader('Accept', equalTo('application/json'))
                .willReturn(aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(504)
                )
        )

        when: "pipe is read and errors"
        client.httpRead(["someType"], 0L, "someLocation")

        then: "pipe service is called 2 times"
        verify(
            2,
            getRequestedFor(urlEqualTo("/pipe/0?type=someType&location=someLocation"))
                .withHeader('Accept', equalTo('application/json'))
        )

        and: "exception is thrown"
        thrown(HttpClientResponseException)

        when: "read again"
        client.httpRead(["someType"], 0L, "someLocation")

        then: "the circuit breaker is open"
        verify(
            2,
            getRequestedFor(urlEqualTo("/pipe/0?type=someType&location=someLocation"))
                .withHeader('Accept', equalTo('application/json'))
        )

        and: "exception is thrown"
        thrown(HttpClientResponseException)

        when: "we sleep longer than the reset time"
        Thread.sleep(1500)

        and: "read again"
        client.httpRead(["someType"], 0L, "someLocation")


        /**
         As per the Micronaut documentation the request should only be called once more if the
         circuit breaker is set to half open. However the behaviour shows that it is called twice.
         **/

        then: "the circuit breaker is set to half open"
        verify(
            4,
            getRequestedFor(urlEqualTo("/pipe/0?type=someType&location=someLocation"))
                .withHeader('Accept', equalTo('application/json'))
        )

        and: "exception is thrown"
        thrown(HttpClientResponseException)
    }
}
