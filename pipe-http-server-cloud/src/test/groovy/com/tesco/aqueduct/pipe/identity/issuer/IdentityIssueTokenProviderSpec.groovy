package com.tesco.aqueduct.pipe.identity.issuer

import com.tesco.aqueduct.pipe.api.IdentityToken
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.convert.value.MutableConvertibleValues
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.simple.SimpleHttpHeaders
import io.reactivex.Single
import io.reactivex.exceptions.CompositeException
import spock.lang.Specification

class IdentityIssueTokenProviderSpec extends Specification {

    public static final String IDENTITY_CLIENT_ID = "identityClientId"
    public static final String IDENTITY_CLIENT_SECRET = "identityClientSecret"

    private IdentityIssueTokenClient identityIssueTokenClient
    private IdentityIssueTokenProvider identityIssueTokenProvider

    void setup() {
        identityIssueTokenClient = Mock()

        identityIssueTokenProvider = new IdentityIssueTokenProvider(
            () -> identityIssueTokenClient,
            IDENTITY_CLIENT_ID,
            IDENTITY_CLIENT_SECRET
        )
    }

    def "token provider returns issued Identity token from Identity client" () {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
            IDENTITY_CLIENT_ID,
            IDENTITY_CLIENT_SECRET
        )

        and:"an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", 10)

        when: "A call is made to retrieve an Identity token"
        IdentityToken identityToken = identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> Single.just(issueTokenResponse)

        and: "token is as expected"
        identityToken.accessToken == "accessToken"
        !identityToken.isTokenExpired()
    }

    def "token is cached if it is not expired" () {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
            IDENTITY_CLIENT_ID,
            IDENTITY_CLIENT_SECRET
        )

        and: "an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", 10)

        when: "a call is made to retrieve an Identity token"
        IdentityToken firstIdentityToken = identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> Single.just(issueTokenResponse)

        and: "token is as expected"
        firstIdentityToken.accessToken == "accessToken"
        !firstIdentityToken.isTokenExpired()

        when: "we fetch the token again"
        IdentityToken secondIdentityToken = identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is not called again"
        0 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest)

        and: "previously fetched token is provided again"
        secondIdentityToken == firstIdentityToken
    }

    def "token is issued again if it has expired"(){
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
            IDENTITY_CLIENT_ID,
            IDENTITY_CLIENT_SECRET
        )

        and: "an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", -1)

        when: "a call is made to retrieve an Identity token"
        IdentityToken firstIdentityToken = identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> Single.just(issueTokenResponse)

        and: "token is expired"
        firstIdentityToken.accessToken == "accessToken"
        firstIdentityToken.isTokenExpired()

        when: "we fetch the token again"
        identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "a new token is issued"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> Single.just(issueTokenResponse)
    }

    def "IdentityServiceUnavailable error thrown when identity client returns 5xx status code"() {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        when: "a call is made to retrieve an Identity token"
        identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is invoked throwing http client response error with 5xx status code"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> {
            Single.error(httpResponseWithStatus(HttpStatus.INTERNAL_SERVER_ERROR))
        }

        and: "IdentityServiceUnavailable error is thrown"
        CompositeException exception = thrown()
        exception.getExceptions().last().class == IdentityServiceUnavailableException
    }

    def "Http client response error is propagated when identity client returns 4xx status code"() {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        when: "a call is made to retrieve an Identity token"
        identityIssueTokenProvider.retrieveIdentityToken().blockingGet()

        then: "issue token client is invoked throwing http client response error with 4xx status code"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> {
            Single.error(httpResponseWithStatus(HttpStatus.BAD_REQUEST))
        }

        and: "IdentityServiceUnavailable error is thrown"
        thrown(HttpClientResponseException)
    }

    private HttpClientResponseException httpResponseWithStatus(HttpStatus httpStatus) {
        new HttpClientResponseException("some error message", new HttpResponse<Object>() {
            @Override
            HttpStatus getStatus() {
                httpStatus
            }

            @Override
            HttpHeaders getHeaders() {
                new SimpleHttpHeaders(ConversionService.SHARED)
            }

            @Override
            MutableConvertibleValues<Object> getAttributes() {
                return null
            }

            @Override
            Optional<Object> getBody() {
                Optional.empty()
            }
        })
    }
}
