package com.tesco.aqueduct.pipe.storage

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

import javax.inject.Inject
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

@MicronautTest
@Property(name="micronaut.caches.latest-offset-cache.expire-after-write", value="1h")
@Property(name="persistence.read.read-delay-seconds", value = "0")
class OffsetFetcherIntegrationSpec extends Specification {

    @Inject
    private ApplicationContext applicationContext

    def "Offset is cached once fetched from db storage"() {
        given:
        def offsetFetcher = applicationContext.getBean(OffsetFetcher)

        and:
        def connection = Mock(Connection)
        def anotherConnection = Mock(Connection)
        def preparedStatement = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)

        when:
        def globalLatestOffset1 = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        1 * connection.prepareStatement(*_) >> preparedStatement
        1 * preparedStatement.executeQuery() >> resultSet
        1 * resultSet.getLong("last_offset") >> 10

        globalLatestOffset1 == 10

        when:
        def globalLatestOffset2 = offsetFetcher.getGlobalLatestOffset(anotherConnection)

        then:
        0 * anotherConnection.prepareStatement(*_)
        globalLatestOffset2 == 10
    }
}
