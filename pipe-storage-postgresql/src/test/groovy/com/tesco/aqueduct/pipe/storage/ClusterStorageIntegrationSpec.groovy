package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.LocationService
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.*
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

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
        
        ALTER TABLE CLUSTERS ADD CONSTRAINT unique_cluster_uuid UNIQUE (cluster_uuid);
        """)

        insertLocationInCache("locationUuid", [1L])

        clusterStorage = new ClusterStorage(dataSource, locationService, Duration.ofMinutes(1))
    }

    def "when cluster cache is hit, clusters ids are returned"() {
        when:
        def clusterIds = clusterStorage.getClusterIds("locationUuid")

        then:
        clusterIds == [1L]

        and: "location service is not called"
        0 * locationService.getClusterUuids("locationUuid")
    }

    def "resolve clusters if location entry is expired"() {
        given: "an expired entry for a location"
        insertLocationInCache("anotherLocationUuid", [1L], Timestamp.valueOf(LocalDateTime.now().minusSeconds(10)))

        when: "cache is read"
        clusterStorage.getClusterIds("anotherLocationUuid")

        then: "location service is called to resolve the cluster ids"
        1 * locationService.getClusterUuids("anotherLocationUuid") >> ["someCluster"]
    }

    def "resolve clusters if location entry is invalidated"() {
        given: "an invalidated entry for a location"
        insertLocationInCache("anotherLocationUuid", [1L], Timestamp.valueOf(LocalDateTime.now().plusSeconds(30)), false)

        when: "cache is read"
        clusterStorage.getClusterIds("anotherLocationUuid")

        then: "location service is called to resolve the cluster ids"
        1 * locationService.getClusterUuids("anotherLocationUuid") >> ["someCluster"]
    }

    def "when there is an error, a runtime exception is thrown"() {
        given: "a datasource and an exception thrown when executing the query"
        def dataSource = Mock(DataSource)
        def connection = Mock(Connection)

        def clusterStorage = new ClusterStorage(dataSource, locationService, Duration.ofMinutes(1))
        dataSource.getConnection() >> connection
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(_) >> preparedStatement
        preparedStatement.executeQuery() >> {throw new SQLException()}

        when: "cluster ids are read"
        clusterStorage.getClusterIds("locationUuid")

        then: "a runtime exception is thrown"
        thrown(RuntimeException)
    }

    def "when location service call fails, then the exception is propagated to the caller"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        locationService.getClusterUuids(anotherLocationUuid) >> { throw new Exception() }

        when:
        clusterStorage.getClusterIds(anotherLocationUuid)

        then:
        thrown(Exception)
    }

    def "DB connection is closed before calling location service when cache is not found"() {
        given: "a datasource and connections"
        def dataSource = Mock(DataSource)
        def connection1 = Mock(Connection)
        def connection2 = Mock(Connection)
        def getCacheQuery = Mock(PreparedStatement)
        def otherQueries = Mock(PreparedStatement)

        and: "location uuid is not cached"
        def uncachedLocationUuid = "uncachedLocationUuid"

        and: "initialized cluster storage with mocks"
        def clusterStorage = new ClusterStorage(dataSource, locationService, Duration.ofMinutes(1))

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
        1 * locationService.getClusterUuids(uncachedLocationUuid) >> ["cluster1", "cluster2"]

        then: "a new connection is created"
        1 * dataSource.getConnection() >> connection2
        3 * connection2.prepareStatement(_) >> otherQueries
        1 * otherQueries.executeQuery() >> Mock(ResultSet)
        1 * connection2.close()
    }

    def "location cache is persisted with the correct expiry time"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"

        when:
        clusterStorage.getClusterIds(anotherLocationUuid)

        then:
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid1", "clusterUuid2"]

        and: "cluster cache is set with correct expiry time"
        def clusterCacheRows = sql.rows("SELECT expiry FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheRows.size() == 1
        clusterCacheRows.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        clusterCacheRows.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))
    }

    def "when location is not cached, then clusters are resolved from location service and persisted in clusters and cache"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"

        when:
        def clusterIds = clusterStorage.getClusterIds(anotherLocationUuid)

        then:
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid1", "clusterUuid2"]

        and: "correct cluster ids are returned"
        clusterIds == [1L, 2L]

        and: "cluster uuids are persisted in clusters table"
        def clusterIdRows = sql.rows("SELECT cluster_id FROM clusters WHERE cluster_uuid in (?,?)", "clusterUuid1", "clusterUuid2")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 1
        clusterIdRows.get(1).get("cluster_id") == 2

        and: "cluster cache is populated correctly"
        def clusterCacheRows = sql.rows("SELECT cluster_ids FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheRows.size() == 1
        Array fetchedClusterIds = clusterCacheRows.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [1L, 2L]
    }

    def "when location entry is invalid, then clusters are resolved from location service and updated in clusters and cache"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Long cluster1 = insertCluster("clusterUuid1")
        Long cluster2 = insertCluster("clusterUuid2")
        insertLocationInCache(anotherLocationUuid, [cluster1, cluster2], Timestamp.valueOf(LocalDateTime.now().plusSeconds(30)), false)

        when:
        def clusterIds = clusterStorage.getClusterIds(anotherLocationUuid)

        then:
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid3", "clusterUuid4"]

        and: "correct cluster ids are returned"
        clusterIds == [3L, 4L]

        and: "cluster uuids are persisted in clusters table"
        List<GroovyRowResult> clusterIdRows = getClusterIdsFor("clusterUuid3", "clusterUuid4")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 3
        clusterIdRows.get(1).get("cluster_id") == 4

        and: "cluster cache is populated correctly"
        def clusterCacheRows = sql.rows("SELECT cluster_ids FROM cluster_cache WHERE location_uuid = ? AND valid = true", anotherLocationUuid)
        clusterCacheRows.size() == 1
        Array fetchedClusterIds = clusterCacheRows.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [3L, 4L]
    }

    def "when location entry is expired, then it is updated with correct clusters and expiry time"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Long cluster1 = insertCluster("clusterUuid1")
        Long cluster2 = insertCluster("clusterUuid2")
        insertLocationInCache(anotherLocationUuid, [cluster1, cluster2], Timestamp.valueOf(LocalDateTime.now().minusSeconds(30)))

        when:
        def clusterIds = clusterStorage.getClusterIds(anotherLocationUuid)

        then: "location service is called to resolve clusters again"
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid1", "clusterUuid2", "clusterUuid3"]

        and: "correct cluster ids are returned"
        // third id will not be 3 but 5 because batch insertion will get conflict on the first two clusters and generated serial will not be inserted
        clusterIds == [1L, 2L, 5L]

        and: "cluster cache is now populated with correct expiry time"
        def clusterCacheRows = sql.rows("SELECT expiry, cluster_ids FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheRows.size() == 1
        clusterCacheRows.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        clusterCacheRows.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))

        and:"expected cluster ids are updated within cluster cache"
        clusterIdsFrom(clusterCacheRows) == [1L, 2L, 5L]

        and: "clusters table is updated with the resolved cluster uuids"
        List<GroovyRowResult> clusterIdRows = getClusterIdsFor("clusterUuid1", "clusterUuid2", "clusterUuid3")
        clusterIdRows.size() == 3
        clusterIdRows.get(0).get("cluster_id") == 1
        clusterIdRows.get(1).get("cluster_id") == 2
        clusterIdRows.get(2).get("cluster_id") == 5
    }

    private List<Long> clusterIdsFrom(List<GroovyRowResult> clusterCacheRows) {
        Array fetchedClusterIds = clusterCacheRows.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[])
    }

    def "when location entry is expired, and invalidated while it is being resolved then it discards last read and resolve clusters again"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Long cluster1 = insertCluster("clusterUuid1")
        Long cluster2 = insertCluster("clusterUuid2")
        insertLocationInCache(anotherLocationUuid, [cluster1, cluster2], Timestamp.valueOf(LocalDateTime.now().minusSeconds(30)))

        when:
        def clusterIds = clusterStorage.getClusterIds(anotherLocationUuid)

        then: "location service is called to resolve clusters again and cache is invalidated while request is in flight"
        1 * locationService.getClusterUuids(anotherLocationUuid) >> {
            invalidateClusterCacheFor(anotherLocationUuid)
            ["clusterUuid1", "clusterUuid2"]
        }

        then: "location service is called again to read the newly published clusters"
        1 * locationService.getClusterUuids(anotherLocationUuid) >> ["clusterUuid3", "clusterUuid4"]

        and: "correct cluster ids are returned"
        // cluster id 3 and 4 will be skipped due to conflict
        clusterIds == [5L, 6L]

        and: "new cluster uuids are persisted in clusters table"
        def clusterIdRows = sql.rows("SELECT cluster_id FROM clusters WHERE cluster_uuid in (?,?)", "clusterUuid3", "clusterUuid4")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 5
        clusterIdRows.get(1).get("cluster_id") == 6

        and: "cluster cache is populated with correct cluster ids and expiry time"
        def updatedClusterCacheRows = sql.rows("SELECT * FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        updatedClusterCacheRows.size() == 1
        updatedClusterCacheRows.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        updatedClusterCacheRows.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))

        Array fetchedClusterIds = updatedClusterCacheRows.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [5L, 6L]
    }

    private List<GroovyRowResult> getClusterIdsFor(String... clusterUuids) {
        def params = Arrays.asList(clusterUuids).stream().map { "?" }.collect(Collectors.joining(","))
        return sql.rows("SELECT cluster_id FROM clusters WHERE cluster_uuid in (" + params + ")", clusterUuids)
    }

    private boolean invalidateClusterCacheFor(String anotherLocationUuid) {
        sql.execute("UPDATE CLUSTER_CACHE SET valid = FALSE WHERE location_uuid = ?", anotherLocationUuid)
    }

    Long insertCluster(String clusterUuid){
        sql.executeInsert("INSERT INTO CLUSTERS(cluster_uuid) VALUES (?);", [clusterUuid]).first()[0]
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
