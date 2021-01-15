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
import java.sql.*
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class ClusterStorageIntegrationSpec extends Specification {
    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    ClusterStorage clusterStorage
    DataSource dataSource
    LocationService locationService = Mock(LocationService)

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

    def "when cluster cache is hit, clusters ids are returned"() {
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

    def "DB connection is closed before calling location service when cache is not found"() {
        given: "a datasource and connections"
        def dataSource = Mock(DataSource)
        def connection1 = Mock(Connection)
        def connection2 = Mock(Connection)
        def getCacheQuery = Mock(PreparedStatement)
        def otherQueries = Mock(PreparedStatement)
/*
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(_) >> preparedStatement
*/

        and: "location uuid is not cached"
        def uncachedLocationUuid = "uncachedLocationUuid"

        and: "initialized cluster storage with mocks"
        def clusterStorage = new ClusterStorage(dataSource, locationService)

        when: "cluster ids are read"
        clusterStorage.getClusterIds(uncachedLocationUuid)

        then: "connection is obtained"
        1 * dataSource.getConnection() >> connection1

        then: "no data found in cache"
        1 * connection1.prepareStatement(_) >> getCacheQuery
        1 * getCacheQuery.executeQuery() >> Mock(ResultSet)

        then: "connection is closed"
        1 * connection1.close()

        then: "location service is invoked"
        1 * locationService.getClusterUuids(uncachedLocationUuid) >> [1, 2]

        then: "a new connection is created"
        1 * dataSource.getConnection() >> connection2
        3 * connection2.prepareStatement(_) >> otherQueries
        1 * otherQueries.executeQuery() >> Mock(ResultSet)
    }

    def "when location is not cached then clusters are resolved from location service and persisted in clusters and cache"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"

        when:
        def clusterIds = clusterStorage.getClusterIds(anotherLocationUuid)

        then:
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid1", "clusterUuid2"]

        and: "correct cluster ids are returned"
        clusterIds.isPresent()
        clusterIds.get() == [1l,2l]

        and: "cluster uuids are persiste in clusters table"
        def clusterIdRows = sql.rows("select cluster_id from clusters where cluster_uuid in (?,?)", "clusterUuid1", "clusterUuid2")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 1
        clusterIdRows.get(1).get("cluster_id") == 2

        and: "cluster cache is populated correctly"
        def clusterCacheRows = sql.rows("select cluster_ids from cluster_cache where location_uuid = ?", anotherLocationUuid)
        clusterCacheRows.size() == 1
        Array fetchedClusterIds = clusterCacheRows.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [1l,2l]
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
