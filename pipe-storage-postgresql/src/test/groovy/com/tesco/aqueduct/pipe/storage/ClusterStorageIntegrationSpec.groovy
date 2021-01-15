package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.LocationService
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Array
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class ClusterStorageIntegrationSpec extends Specification {
    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    ClusterStorage clusterStorage
    DataSource dataSource
    LocationService locationService

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
        DROP TABLE IF EXISTS CLUSTERS;
        DROP TABLE IF EXISTS CLUSTER_CACHE;
          
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        CREATE TABLE CLUSTER_CACHE(
            location_uuid VARCHAR PRIMARY KEY NOT NULL,
            cluster_ids BIGINT[] NOT NULL,
            expiry TIMESTAMP NOT NULL,
            valid BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)

        insertLocationInCache("locationUuid", [1L])

        clusterStorage = new ClusterStorage(dataSource, locationService)
    }

    def "when cluster cached is hit, clusters ids are returned"() {
        when:
        def clusterIds = clusterStorage.getClusterIds("locationUuid")

        then:
        clusterIds == Optional.of([1L])
    }

    def "when there is an error, a runtime exception is thrown"() {
        given: "a datasource and an exception thrown when executing the query"
        def dataSource = Mock(DataSource)
        def connection = Mock(Connection)

        def clusterStorage = new ClusterStorage(dataSource, locationService)
        dataSource.getConnection() >> connection
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(_) >> preparedStatement
        preparedStatement.executeQuery() >> {throw new SQLException()}

        when: "cluster ids are read"
        clusterStorage.getClusterIds("locationUuid")

        then: "a runtime exception is thrown"
        thrown(RuntimeException)
    }

    void insertLocationInCache(
        String locationUuid,
        List<Long> clusterIds,
        def expiry = Timestamp.valueOf(LocalDateTime.now() + TimeUnit.MINUTES.toMillis(1)),
        boolean valid = true
    ) {
        Connection connection = DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        Array clusters = connection.createArrayOf("integer", clusterIds.toArray())
        sql.execute(
                "INSERT INTO CLUSTER_CACHE(location_uuid, cluster_ids, expiry, valid) VALUES (?, ?, ?, ?)",
                locationUuid, clusters, expiry, valid
        )
    }
}
