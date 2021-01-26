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
    LocationService locationService = Mock(LocationService)
    Connection connection

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        connection = DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

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
        insertCluster("someClusterUuid")

        clusterStorage = new ClusterStorage(locationService, Duration.ofMinutes(1))
    }

    def "when cluster cache is read successfully, a cluster cache optional containing results is returned"() {
        when:
        def cacheResult = clusterStorage.getClusterCacheEntry("locationUuid", connection)

        then:
        cacheResult.isPresent()
        def entry = cacheResult.get()
        entry.isValid()
        entry.getClusterIds() == [1L]
        entry.getExpiry() > LocalDateTime.now()

        and:"connection is not closed"
        !connection.isClosed()
    }

    def "return optional empty when location is not cached"() {
        when: "cache is read"
        def cacheResults = clusterStorage.getClusterCacheEntry("anotherLocationUuid", connection)

        then:
        !cacheResults.isPresent()

        and:"connection is not closed"
        !connection.isClosed()
    }

    def "when an exception is thrown while prepareStatement operation during cache read, a runtime exception is propagated"() {
        given:
        Connection connection = Mock(Connection)
        connection.prepareStatement(_) >> { throw new SQLException() }

        when: "cache is read"
        clusterStorage.getClusterCacheEntry("anotherLocationUuid", connection)

        then:
        def exception = thrown(RuntimeException)

        and:
        exception.getCause() instanceof SQLException
    }

    def "resolve clusters from location service for a given location id"() {
        when: "resolving clusteruuids for the given location"
        def clustersUuids = clusterStorage.resolveClustersFor("someLocationUuid")

        then: "location service is called to resolve the cluster ids"
        1 * locationService.getClusterUuids("someLocationUuid") >> ["someClusterUuid1", "someClusterUuid2"]

        and:
        clustersUuids == ["someClusterUuid1", "someClusterUuid2"]
    }

    def "when cluster resolution fails, an exception is thrown to the caller"() {
        given: "an exception is thrown when calling location service"
        locationService.getClusterUuids("someLocationUuid") >> { throw new Exception() }

        when:
        clusterStorage.resolveClustersFor("someLocationUuid")

        then: "the exception is thrown to the caller"
        thrown(Exception)
    }

    def "when there is an error during cache read while executing the query, a runtime exception is propagated"() {
        given: "a datasource and an exception thrown when executing the query"
        def dataSource = Mock(DataSource)
        def connection = Mock(Connection)

        def clusterStorage = new ClusterStorage(locationService, Duration.ofMinutes(1))
        dataSource.getConnection() >> connection
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(_) >> preparedStatement
        preparedStatement.executeQuery() >> {throw new SQLException()}

        when: "cluster ids are read"
        clusterStorage.getClusterCacheEntry("locationUuid", connection)

        then:
        def exception = thrown(RuntimeException)

        and:
        exception.getCause() instanceof SQLException
    }

    def "cluster cache is updated with given clusters when cache not present"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"

        when:
        def clusterIds = clusterStorage.updateAndGetClusterIds(anotherLocationUuid, ["clusterUuid1", "clusterUuid2"], Optional.empty(), connection)

        then: "correct cluster ids are returned"
        clusterIds == Optional.of([2L, 3L])

        and: "cluster uuids are persisted in clusters table"
        def clusterIdRows = sql.rows("SELECT cluster_id FROM clusters WHERE cluster_uuid in (?,?)", "clusterUuid1", "clusterUuid2")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 2
        clusterIdRows.get(1).get("cluster_id") == 3

        and: "cluster cache is populated correctly"
        def clusterCacheEntry = sql.rows("SELECT cluster_ids, expiry FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheEntry.size() == 1
        Array fetchedClusterIds = clusterCacheEntry.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [2L, 3L]

        and: "expiry time is set correctly"
        clusterCacheEntry.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        clusterCacheEntry.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))
    }

    def "cluster cache is updated with given clusters when location cache entry is invalid"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Optional<ClusterCacheEntry> entry = Optional.of(new ClusterCacheEntry("anotherLocationUuid", [], LocalDateTime.now(), false))

        when:
        def clusterIds = clusterStorage.updateAndGetClusterIds(anotherLocationUuid, ["clusterUuid1", "clusterUuid2"], entry, connection)

        then: "correct cluster ids are returned"
        clusterIds == Optional.of([2L, 3L])

        and: "cluster uuids are persisted in clusters table"
        List<GroovyRowResult> clusterIdRows = getClusterIdsFor("clusterUuid1", "clusterUuid2")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 2
        clusterIdRows.get(1).get("cluster_id") == 3

        and: "cluster cache is populated correctly"
        def clusterCacheEntry = sql.rows("SELECT cluster_ids, expiry FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheEntry.size() == 1
        Array fetchedClusterIds = clusterCacheEntry.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[]) == [2L, 3L]

        and: "expiry time is set correctly"
        clusterCacheEntry.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        clusterCacheEntry.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))
    }

    def "when location entry is expired, then it is updated with correct clusters and expiry time"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Optional<ClusterCacheEntry> entry = Optional.of(new ClusterCacheEntry("anotherLocationUuid", [2L], LocalDateTime.now().minusMinutes(1), true))

        insertCluster("clusterUuid1")
        insertLocationInCache(anotherLocationUuid, [2L],  LocalDateTime.now().minusMinutes(1))

        when:
        def clusterIds = clusterStorage.updateAndGetClusterIds(anotherLocationUuid, ["clusterUuid1", "clusterUuid2"], entry, connection)

        then: "correct cluster ids are returned"
        // second id will not be 3 but 4 because batch insertion will get conflict on the first cluster and generated serial will not be inserted
        clusterIds == Optional.of([2L, 4L])

        and: "cluster cache is now populated with correct expiry time"
        def clusterCacheEntry = sql.rows("SELECT expiry, cluster_ids FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheEntry.size() == 1
        clusterCacheEntry.get(0).get("expiry") > Timestamp.valueOf(LocalDateTime.now().plusSeconds(59))
        clusterCacheEntry.get(0).get("expiry") < Timestamp.valueOf(LocalDateTime.now().plusSeconds(61))

        and:"expected cluster ids are updated within cluster cache"
        clusterIdsFrom(clusterCacheEntry) == [2L, 4L]

        and: "clusters table is updated with the resolved cluster uuids"
        List<GroovyRowResult> clusterIdRows = getClusterIdsFor("clusterUuid1", "clusterUuid2")
        clusterIdRows.size() == 2
        clusterIdRows.get(0).get("cluster_id") == 2
        clusterIdRows.get(1).get("cluster_id") == 4
    }

    def "when location cache entry is expired and invalidated while being resolved, then empty optional is returned"() {
        given:
        def anotherLocationUuid = "anotherLocationUuid"
        Optional<ClusterCacheEntry> entry = Optional.of(new ClusterCacheEntry("anotherLocationUuid", [2L], LocalDateTime.now().minusMinutes(1), true))

        insertCluster("clusterUuid1")
        insertLocationInCache(anotherLocationUuid, [2L],  LocalDateTime.now().minusMinutes(1), false)

        when:
        def clusterIds = clusterStorage.updateAndGetClusterIds(anotherLocationUuid, ["clusterUuid1"], entry, connection)

        then: "optional empty is returned"
        clusterIds == Optional.empty()

        and: "cluster cache has not been populated"
        def clusterCacheEntry = sql.rows("SELECT * FROM cluster_cache WHERE location_uuid = ? AND valid = TRUE", anotherLocationUuid)
        clusterCacheEntry.size() == 0

        and: "clusters table is updated with the resolved cluster uuids"
        List<GroovyRowResult> clusterIdRows = getClusterIdsFor("clusterUuid1")
        clusterIdRows.size() == 1
        clusterIdRows.get(0).get("cluster_id") == 2
    }

    private List<Long> clusterIdsFrom(List<GroovyRowResult> clusterCacheEntry) {
        Array fetchedClusterIds = clusterCacheEntry.get(0).get("cluster_ids") as Array
        Arrays.asList(fetchedClusterIds.getArray() as Long[])
    }

    private List<GroovyRowResult> getClusterIdsFor(String... clusterUuids) {
        def params = Arrays.asList(clusterUuids).stream().map { "?" }.collect(Collectors.joining(","))
        return sql.rows("SELECT cluster_id FROM clusters WHERE cluster_uuid in (" + params + ")", clusterUuids)
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
