package com.tesco.aqueduct.pipe.storage

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource

class PostgresqlStorageSpec extends Specification {

    @Shared
    def retryAfter = 30000
    LocationResolver locationResolver = Mock(LocationResolver)

    @Unroll
    def "retry after is #result when queryTime is #timeOfQueryMs and message result size is #noOfMessages"() {
        given:
        def readersNodeCount = 3000
        def clusterDBPoolSize = 60

        and:
        def storage = new PostgresqlStorage(Mock(DataSource), 20, retryAfter, 2, new OffsetFetcher(0), readersNodeCount, clusterDBPoolSize, 4, locationResolver)

        expect:
        storage.calculateRetryAfter(timeOfQueryMs, noOfMessages) == result

        where:
        timeOfQueryMs | noOfMessages | result
        100           | 10000        | 5000
        200           | 10000        | 10000
        10            | 10000        | 500
        50            | 10000        | 2500
        0             | 10000        | 1
        1             | 10000        | 50
    }

    @Unroll
    def "retry after is retryAfter*1000 when queryTime is #timeOfQueryMs and message result size is #noOfMessages"() {
        given:
        def readersNodeCount = 3000
        def clusterDBPoolSize = 60

        and:
        def storage = new PostgresqlStorage(Mock(DataSource), 20, retryAfter, 2, new OffsetFetcher(0), readersNodeCount, clusterDBPoolSize, 4, locationResolver)

        expect:
        storage.calculateRetryAfter(timeOfQueryMs, noOfMessages) >= result

        where:
        timeOfQueryMs | noOfMessages | result
        100           | 0            | retryAfter
        1234          | 0            | retryAfter
        700           | 10000        | retryAfter
        1000          | 10000        | retryAfter
    }
}
