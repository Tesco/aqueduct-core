package com.tesco.aqueduct.pipe.storage

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

class PostgresqlStorageSpec extends Specification {

    @Shared
    def retryAfter = 30000
    ClusterStorage clusterStorage = Mock(ClusterStorage)

    @Unroll
    def "retry after is #result when queryTime is #timeOfQueryMs and message result size is #noOfMessages"() {
        given:
        def readersNodeCount = 3000
        def clusterDBPoolSize = 60

        and:
        def storage = new PostgresqlStorage(Mock(DataSource), Mock(DataSource), 20, retryAfter, 2, new OffsetFetcher(0), readersNodeCount, clusterDBPoolSize, 4, clusterStorage)

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
        def storage = new PostgresqlStorage(Mock(DataSource), Mock(DataSource), 20, retryAfter, 2, new OffsetFetcher(0), readersNodeCount, clusterDBPoolSize, 4, clusterStorage)

        expect:
        storage.calculateRetryAfter(timeOfQueryMs, noOfMessages) >= result

        where:
        timeOfQueryMs | noOfMessages | result
        100           | 0            | retryAfter
        1234          | 0            | retryAfter
        700           | 10000        | retryAfter
        1000          | 10000        | retryAfter
    }

    def "Exception thrown during connection close is propagated upstream"() {
        given:
        def dataSource = Mock(DataSource)
        def storage = new PostgresqlStorage(dataSource, Mock(DataSource), 20, retryAfter, 2, new OffsetFetcher(0), 1000, 4, 4, clusterStorage)

        and:
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and:
        clusterStorage.getClusterCacheEntry("someLocationUuid", connection) >> Optional.empty()

        and:
        connection.close() >> {throw new SQLException()}

        when:
        storage.read([], 0, "someLocationUuid")

        then:
        def exception = thrown(RuntimeException)
        exception.getCause() instanceof SQLException
    }
}
