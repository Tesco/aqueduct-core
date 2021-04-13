package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.PipeState
import groovy.sql.Sql
import groovy.transform.NamedVariant
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import javax.sql.DataSource
import java.sql.*
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PostgresqlStorageIntegrationSpec extends StorageSpec {

    // Starts real PostgreSQL database, takes some time to create it and clean it up.
    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    PostgresqlStorage storage
    DataSource dataSource
    ClusterStorage clusterStorage

    long retryAfter = 5000
    long batchSize = 1000
    long maxOverheadBatchSize = (Message.MAX_OVERHEAD_SIZE * limit) + batchSize

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS EVENTS_BUFFER;
        DROP TABLE IF EXISTS CLUSTERS;
        DROP TABLE IF EXISTS REGISTRY;
        DROP TABLE IF EXISTS NODE_REQUESTS;
        DROP TABLE IF EXISTS OFFSETS;
        DROP TABLE IF EXISTS LOCKS;
        DROP TABLE IF EXISTS LOCATION_GROUPS;
          
        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1,
            location_group BIGINT,
            time_to_live TIMESTAMP NULL
        );        
        
        CREATE TABLE EVENTS_BUFFER(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key VARCHAR NOT NULL,
            content_type VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            created_utc TIMESTAMP NOT NULL,
            data TEXT NULL,
            event_size INT NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1,
            time_to_live TIMESTAMP NULL
        ); 
        
        CREATE TABLE NODE_REQUESTS(
            host_id VARCHAR PRIMARY KEY NOT NULL,
            bootstrap_requested timestamp NOT NULL,
            bootstrap_type VARCHAR NOT NULL,
            bootstrap_received timestamp
        );
        
        CREATE TABLE REGISTRY(
            group_id VARCHAR PRIMARY KEY NOT NULL,
            entry JSON NOT NULL
        );
        
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        CREATE TABLE OFFSETS(
            name VARCHAR PRIMARY KEY NOT NULL,
            value BIGINT NOT NULL
        );
        
        CREATE TABLE LOCKS(
            name VARCHAR PRIMARY KEY
        );

        CREATE TABLE LOCATION_GROUPS(
            location_uuid VARCHAR PRIMARY KEY,
            groups BIGINT[] NOT NULL
        );

        INSERT INTO LOCKS (name) VALUES ('maintenance_lock');
        INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');        
        """)

        clusterStorage = Mock(ClusterStorage)
        clusterStorage.getClusterCacheEntry("locationUuid", _ as Connection) >> cacheEntry("locationUuid", [1L])
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, new OffsetFetcher(0), 1, 1, 4, clusterStorage)
    }

    @Unroll
    def "get #offsetName returns max offset"() {
        given: "there are messages"
        def msg1 = message(offset: 1)
        def msg2 = message(offset: 2)
        def msg3 = message(offset: 3)

        and: "they are inserted into the integrated database"
        insert(msg1, 10)
        insert(msg2, 10)
        insert(msg3, 10)

        when: "reading latest offset from the database"
        def offset = storage.getOffset(offsetName)

        then: "offset should be the offset of latest message in the storage"
        offset.getAsLong() == 3

        where:
        offsetName << [OffsetName.GLOBAL_LATEST_OFFSET, OffsetName.PIPE_OFFSET, OffsetName.LOCAL_LATEST_OFFSET]
    }

    def "get pipe state as up to date always"() {
        when: "reading the messages"
        def messageResults = storage.read(["some_type"], 0, "locationUuid")

        then: "pipe state is up to date"
        messageResults.pipeState == PipeState.UP_TO_DATE
    }

    def "get messages for given type and location"() {
        given: "there is postgres storage"
        def limit = 1
        def dataSourceWithMockedConnection = Mock(DataSource)
        def postgresStorage = new PostgresqlStorage(dataSourceWithMockedConnection, dataSourceWithMockedConnection, limit, 0, batchSize, new OffsetFetcher(0), 1, 1, 4, clusterStorage)

        and: "a mock connection is provided when requested"
        def connection = Mock(Connection)
        dataSourceWithMockedConnection.getConnection() >> connection

        and: "a connection returns a prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getArray(1) >> Mock(Array)
        connection.prepareStatement(_ as String) >> preparedStatement

        when: "requesting messages specifying a type key and a locationUuid"
        postgresStorage.read(["some_type"], 0, "locationUuid")

        then: "a query is created that contain given type and location"
        2 * preparedStatement.setLong(_, 0)
        1 * preparedStatement.setString(_, "some_type")
    }

    def "the messages returned are no larger than the maximum batch size when reading without a type"() {
        given: "there are messages with unique keys"
        def msg1 = message(key: "x")
        def msg2 = message(key: "y")
        def msg3 = message(key: "z")

        and: "the size of each message is set so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "they are inserted into the integrated database"
        insert(msg1, 1, messageSize)
        insert(msg2, 1, messageSize)
        insert(msg3, 1, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read([], 0, "locationUuid")

        then: "messages that are returned are no larger than the maximum batch size when reading with a type"
        result.messages.size() == 2
    }

    def "the messages returned are no larger than the maximum batch size"() {
        given: "there are messages with unique keys"
        def msg1 = message(key: "x", type: "type-1")
        def msg2 = message(key: "y", type: "type-1")
        def msg3 = message(key: "z", type: "type-1")

        and: "the size of each message is set so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "they are inserted into the integrated database"
        insert(msg1, 1, messageSize)
        insert(msg2, 1, messageSize)
        insert(msg3, 1, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read(["type-1"], 0, "locationUuid")

        then: "messages that are returned are no larger than the maximum batch size"
        result.messages.size() == 2
    }

    def "retry-after is non-zero if the pipe has no more data at specified offset"() {
        given: "I have some records in the integrated database"
        insert(message(key: "z"))
        insert(message(key: "y"))
        insert(message(key: "x"))

        when:
        MessageResults result = storage.read([], 4, "locationUuid")

        then:
        result.retryAfterMs > 0
        result.messages.isEmpty()
    }

    def "retry-after is non-zero if the pipe has no data"() {
        given: "I have no records in the integrated database"

        when:
        MessageResults result = storage.read([], 0,"locationUuid")

        then:
        result.retryAfterMs > 0
        result.messages.isEmpty()
    }

    def "Messages with TTL set to future are not compacted"() {
        given: "messages stored with cluster id and TTL set to today"
        insertWithClusterAndTTL(1, "A", 1, LocalDateTime.now().plusMinutes(60))
        insertWithClusterAndTTL(2, "A", 1, LocalDateTime.now().plusMinutes(60))
        insertWithClusterAndTTL(3, "A", 1, LocalDateTime.now().plusMinutes(60))

        when: "compaction with ttl is run"
        storage.compactAndMaintain()

        and: "all messages are read"
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then: "no messages are compacted"
        retrievedMessages.size() == 3
    }

    def "Messages with TTL in the past are compacted"() {
        given: "messages stored with cluster id and TTL set to today"
        insertWithClusterAndTTL(1, "A", 1, LocalDateTime.now().minusMinutes(60))
        insertWithClusterAndTTL(2, "A", 1, LocalDateTime.now().minusMinutes(10))
        insertWithClusterAndTTL(3, "A", 1, LocalDateTime.now().minusMinutes(1))

        when: "compaction with ttl is run"
        storage.compactAndMaintain()

        and: "all messages are read"
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then: "all messages are compacted"
        retrievedMessages.size() == 0
    }

    def "Compaction only runs once when called in parallel"() {
        given: "database with lots of data ready to be compacted"
        100000.times{i ->
            insertWithClusterAndTTL(i, "A", 1, LocalDateTime.now().minusMinutes(60))
        }

        and: "multiple threads attempting to compact in parallel"
        def completed = [] as Set
        def compactionRan = [] as Set

        ExecutorService pool = Executors.newFixedThreadPool(5)
        CompletableFuture startLock = new CompletableFuture()

        5.times{ i ->
            pool.execute{
                startLock.get()
                boolean compacted = storage.compactAndMaintain()
                completed.add(i)

                if(compacted) {
                    compactionRan.add(i)
                }
            }
        }

        startLock.complete(true)

        expect: "compaction only ran once"
        PollingConditions conditions = new PollingConditions(timeout: 5)

        conditions.eventually {
            completed.size() == 5
            compactionRan.size() == 1
        }
    }

    def "locking fails gracefully when lock cannot be obtained"() {
        given: "lock is held by the test"
        Connection connection = sql.connection
        connection.setAutoCommit(false)
        Statement statement = connection.prepareStatement("SELECT * from locks where name='maintenance_lock' FOR UPDATE NOWAIT;")
        print statement.execute()

        when: "call compact"
        boolean gotLock = storage.attemptToLock(DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres")))

        then: "compaction didnt happen"
        !gotLock
    }

    def "Variety of TTL value messages are compacted correctly"() {
        given: "messages stored with cluster id and TTL set to today"
        insertWithClusterAndTTL(1, "A", 1, LocalDateTime.now().plusMinutes(60))
        insertWithClusterAndTTL(2, "A", 1, LocalDateTime.now().minusMinutes(10))
        insertWithClusterAndTTL(3, "A", 1, LocalDateTime.now().minusMinutes(1))

        when: "compaction with ttl is run"
        storage.compactAndMaintain()

        and: "all messages are read"
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then: "correct messages are compacted"
        retrievedMessages.size() == 1
    }

    def "Messages with null TTL arent compacted"() {
        given: "messages stored with cluster id and null TTL"
        insert(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "compaction with ttl is run"
        storage.compactAndMaintain()

        and: "all messages are read"
        MessageResults result = storage.read(null, 0, "locationUuid")
        List<Message> retrievedMessages = result.messages

        then: "no messages are compacted"
        retrievedMessages.size() == 3
    }

    @Unroll
    def "Global latest offset is returned"() {
        given: "an existing data store with two different types of messages"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "reading all messages"
        def messageResults = storage.read([type], 0, "locationUuid")

        then: "global latest offset is type and clusterId independent"
        messageResults.globalLatestOffset == OptionalLong.of(3)

        where:
        type << ["type1", "type2", "type3"]
    }

    def "pipe should return all messages when no types are provided and all messages have default cluster"(){
        given: "some messages are stored with default cluster"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "reading with no types"
        def messageResults = storage.read([], 0, "locationUuid")

        then: "all messages from the storage are returned regardless the types"
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
    }

    def "pipe should return messages for given clusters and no type"() {
        given: "some messages are stored with cluster ids"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2)
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)

        when: "reading with no types but cluster provided"
        def messageResults = storage.read([], 0, "locationUuid")

        then: "messages belonging to cluster1 are returned"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "C"]
        messageResults.messages*.offset*.intValue() == [1, 3]
    }

    def "pipe should return relevant messages when types and cluster are provided"(){
        given: "some messages are stored with cluster ids"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2)
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2)
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(6, "type3", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)

        when: "reading with no types but cluster provided"
        def messageResults = storage.read(["type2", "type3"], 0, "locationUuid")

        then: "messages belonging to cluster1 are returned"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["D", "F"]
        messageResults.messages*.offset*.intValue() == [4, 6]
    }

    def "no messages are returned when cluster does not map to any messages"() {
        given: "some messages are stored"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2)
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2)
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)
        insert(message(6, "type3", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1)

        when: "reading with a location having no messages mapped to its clusters"
        def messageResults = storage.read(["type2", "type3"], 0, "location2")

        then: "messages are not returned, and no exception is thrown"
        1 * clusterStorage.getClusterCacheEntry("location2", _ as Connection) >> cacheEntry("location2", [3, 4])
        messageResults.messages.size() == 0
        noExceptionThrown()
    }

    def "pipe should return messages if available from the given offset instead of empty set"() {
        given: "there is postgres storage"
        def limit = 3
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, new OffsetFetcher(0), 1, 1, 4, clusterStorage)

        and: "an existing data store with two different types of messages"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type2", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type2", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(9, "type1", "I", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "reading all messages"
        def messageResults = storage.read(["type1"], 0, "locationUuid")

        then: "messages are provided for the given type"
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
        messageResults.globalLatestOffset == OptionalLong.of(9)

        when: "read again from further offset"
        messageResults = storage.read(["type1"], 4, "locationUuid")

        then: "we should still get relevant messages back even if they are further down from the given offset"
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["G", "H", "I"]
        messageResults.messages*.offset*.intValue() == [7, 8, 9]
        messageResults.globalLatestOffset == OptionalLong.of(9)
    }

    def "getMessageCountByType should return the count of messages by type"() {
        given: "there is postgres storage"
        def limit = 3
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, new OffsetFetcher(0), 1, 1, 4, clusterStorage)

        and: "an existing data store with two different types of messages"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type2", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type2", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(9, "type1", "I", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "getMessageCountByType is called"
        Map<String, Long> result = storage.getMessageCountByType(dataSource.connection)

        then: "the correct count is returned"
        result.get("type1") == 6
        result.get("type2") == 3

        noExceptionThrown()
    }

    @Unroll
    def "when messages have out of order created_utc, we read up to the message with minimum offset outside the limit"() {
        given:
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        // messages out of order
        insert(message(4, "type1", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))
        insert(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type1", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))

        when: "reading all messages"
        def messageResults = storage.read(types, 1, "locationUuid")

        then: "messages are provided for the given type"
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
        messageResults.globalLatestOffset == OptionalLong.of(3)

        where:
        types << [ [], ["type1"] ]
    }

    def "messages are returned when location uuid is contained and valid in the cluster cache"() {
        given:
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)

        clusterStorage.getClusterCacheEntry("someLocationUuid", _ as Connection) >> cacheEntry("someLocationUuid", [2L, 3L])
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2L)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L)
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 4L)

        when: "reading all messages"
        def messageResults = storage.read(["type1"], 0, "someLocationUuid")

        then: "messages are provided for the given location"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "B"]
        messageResults.messages*.offset*.intValue() == [1, 2]
        messageResults.globalLatestOffset == OptionalLong.of(3)
    }

    def "Clusters are resolved and cache is populated when cache is missing clusters for the given location during read"() {
        given:
        dataSource = Mock()
        def connection1 = DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        def connection2 = DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and:
        clusterStorage = Mock(ClusterStorage)

        and:
        def someLocationUuid = "someLocationUuid"
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2L)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L)

        when: "reading all messages with a location"
        def messageResults = storage.read(["type1"], 0, someLocationUuid)

        then: "First connection is obtained"
        1 * dataSource.connection >> connection1

        then: "clusters for given location are not cached"
        1 * clusterStorage.getClusterCacheEntry(someLocationUuid, connection1) >> Optional.empty()

        then: "First connection is closed"
        connection1.isClosed()

        then:
        1 * clusterStorage.resolveClustersFor(someLocationUuid) >> ["clusterUuid2", "clusterUuid3"]

        then: "Second connection is obtained"
        1 * dataSource.connection >> connection2

        then: "location cache is populated"
        1 * clusterStorage.updateAndGetClusterIds(someLocationUuid, ["clusterUuid2", "clusterUuid3"], Optional.empty(), connection2) >> [2L, 3L]

        then: "messages are provided for the given location"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "B"]
        messageResults.messages*.offset*.intValue() == [1, 2]
        messageResults.globalLatestOffset == OptionalLong.of(2)
    }

    def "Any exception during cache read is propagated upstream"() {
        given:
        clusterStorage = Mock(ClusterStorage)

        and:
        clusterStorage.getClusterCacheEntry(*_) >> {throw new RuntimeException(new SQLException())}

        when: "reading all messages with a location"
        storage.read(["type1"], 0, "someLocationUuid")

        then: "First connection is obtained"
        thrown(RuntimeException)
    }

    def "Read is performed twice when cluster cache is invalidated while location service request is in flight"() {
        given:
        def someLocationUuid = "someLocationUuid"
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)
        def firstCacheRead = cacheEntry(someLocationUuid, [1L], LocalDateTime.now().minusMinutes(1))
        def secondCacheRead = cacheEntry(someLocationUuid, [1L], LocalDateTime.now().plusMinutes(1), false)

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2L)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L)

        when: "reading all messages with a location"
        def messageResults = storage.read(["type1"], 0, someLocationUuid)

        then: "cluster cache is expired"
        1 * clusterStorage.getClusterCacheEntry(someLocationUuid, _ as Connection) >> firstCacheRead

        then: "clusters are resolved from location service"
        1 * clusterStorage.resolveClustersFor(someLocationUuid) >> ["clusterUuid2", "clusterUuid3"]

        then: "cache is invalidated hence returns no result"
        1 * clusterStorage.updateAndGetClusterIds(someLocationUuid, ["clusterUuid2", "clusterUuid3"], firstCacheRead, _ as Connection) >> Optional.empty()

        then: "clusters are resolved again"
        1 * clusterStorage.getClusterCacheEntry(someLocationUuid, _ as Connection) >> secondCacheRead
        1 * clusterStorage.resolveClustersFor(someLocationUuid) >> ["clusterUuid2", "clusterUuid3"]
        1 * clusterStorage.updateAndGetClusterIds(someLocationUuid, ["clusterUuid2", "clusterUuid3"], secondCacheRead, _ as Connection) >> [2L, 3L]

        then: "messages are provided for the given location"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "B"]
        messageResults.messages*.offset*.intValue() == [1, 2]
        messageResults.globalLatestOffset == OptionalLong.of(2)
    }

    def "clusters are resolved from location service and updated when cluster cache is expired"() {
        given:
        def someLocationUuid = "someLocationUuid"
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)
        def cacheRead = cacheEntry(someLocationUuid, [2L, 3L], LocalDateTime.now().minusMinutes(1))

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2L)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L)

        when: "reading all messages with a location"
        def messageResults = storage.read(["type1"], 0, someLocationUuid)

        then: "cluster cache is expired"
        1 * clusterStorage.getClusterCacheEntry(someLocationUuid, _ as Connection) >> cacheRead

        then: "clusters are resolved from location service"
        1 * clusterStorage.resolveClustersFor(someLocationUuid) >> ["clusterUuid2", "clusterUuid3"]

        then: "cache is invalidated hence returns no result"
        1 * clusterStorage.updateAndGetClusterIds(someLocationUuid, ["clusterUuid2", "clusterUuid3"], cacheRead, _ as Connection) >> Optional.of([2L, 3L])

        then: "messages are provided for the given location"
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "B"]
        messageResults.messages*.offset*.intValue() == [1, 2]
        messageResults.globalLatestOffset == OptionalLong.of(2)
    }

    def "Exception during cache update is propagated upstream"() {
        given:
        def someLocationUuid = "someLocationUuid"
        def offsetFetcher = new OffsetFetcher(0)
        offsetFetcher.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"
        storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, offsetFetcher, 1, 1, 4, clusterStorage)
        def cacheRead = cacheEntry(someLocationUuid, [2L, 3L], LocalDateTime.now().minusMinutes(1))

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 2L)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L)

        when: "reading all messages with a location"
        storage.read(["type1"], 0, someLocationUuid)

        then: "cluster cache is expired"
        1 * clusterStorage.getClusterCacheEntry(someLocationUuid, _ as Connection) >> cacheRead

        then: "clusters are resolved from location service"
        1 * clusterStorage.resolveClustersFor(someLocationUuid) >> ["clusterUuid2", "clusterUuid3"]

        then: "cache is invalidated hence returns no result"
        1 * clusterStorage.updateAndGetClusterIds(someLocationUuid, ["clusterUuid2", "clusterUuid3"], cacheRead, _ as Connection) >> { throw new RuntimeException() }

        then: "messages are provided for the given location"
        thrown(RuntimeException)
    }


    def "Any exception during cache resolution from location service is propagated upstream"() {
        given:
        clusterStorage = Mock(ClusterStorage)

        and:
        clusterStorage.getClusterCacheEntry(*_) >> Optional.empty()

        and:
        clusterStorage.resolveClustersFor("someLocationUuid") >> {throw new RuntimeException()}

        when: "reading all messages with a location"
        storage.read(["type1"], 0, "someLocationUuid")

        then: "First connection is obtained"
        thrown(RuntimeException)
    }

    @Unroll
    def "read up to the last message in the pipe when all are in the visibility window"() {
        given:
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: "reading all messages"
        def messageResults = storage.read(types, 1, "locationUuid")

        then: "messages are provided for the given type"
        messageResults.messages.size() == 1
        messageResults.messages*.key == ["A"]
        messageResults.messages*.offset*.intValue() == [1]
        messageResults.globalLatestOffset == OptionalLong.of(1)

        where:
        types << [ [], ["type1"] ]
    }

    def "messages with location group are not read if given location is not part of any groups"() {
        given: "2 messages, one of which with a location group that the location does not belong to"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), 2L)

        and: "location does not belong to the group"
        insertLocationGroupFor("locationUuid", [])

        when: "messages are read"
        def messageResults = storage.read([], 0L, "locationUuid")

        then: "only the message belonging to the location cluster is returned"
        messageResults.messages.size() == 1
        messageResults.messages.get(0).offset == 1
    }

    def "messages with location group are not read if location is not present in location groups table"() {
        given: "2 messages, one of which with a location group that the location does not belong to"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), 2L)

        when: "messages are read"
        def messageResults = storage.read([], 0L, "locationUuid")

        then: "only the message belonging to the location cluster is returned"
        messageResults.messages.size() == 1
        messageResults.messages.get(0).offset == 1
    }

    def "messages for the given cluster and groups for a given location are both read"() {
        given: "2 messages, one of which with location group"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), 1L)

        and: "location belongs to the group"
        insertLocationGroupFor("locationUuid", [1L])

        when: "messages are read"
        def messageResults = storage.read([], 0L, "locationUuid")

        then: "both messages are returned"
        messageResults.messages.size() == 2
        messageResults.messages.offset*.intValue() == [1, 2]
    }

    def "location groups are resolved correctly as part of the read"() {
        given: "a location and its group"
        def locationUuid = "some_location"
        def clusterId = 3L
        clusterStorage.getClusterCacheEntry(locationUuid, _ as Connection) >> cacheEntry(locationUuid, [clusterId])
        insertLocationGroupFor(locationUuid, [3L, 4L])

        and: "messages are stored"
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), null)
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), 1L)
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L, 0, Timestamp.valueOf(time.toLocalDateTime()), null)
        insert(message(4, "type1", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L, 0, Timestamp.valueOf(time.toLocalDateTime()), 3L)
        insert(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 1L, 0, Timestamp.valueOf(time.toLocalDateTime()), 3L)
        insert(message(6, "type1", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), 3L, 0, Timestamp.valueOf(time.toLocalDateTime()), 4L)

        when: "messages are read"
        def messageResults = storage.read([], 0L, locationUuid)

        then: "only messages for the relevant cluster and group are returned"
        messageResults.messages.size() == 3
        messageResults.messages.offset*.intValue() == [3, 4, 6]
        messageResults.globalLatestOffset == OptionalLong.of(6)
    }

    def "vacuum analyse query is valid"() {
        given: "a database"

        when: "vacuum analyse is called via compact and maintain method"
        storage.compactAndMaintain()

        then: "no exception thrown"
        noExceptionThrown()
    }

    void insert(
        Message msg,
        Long clusterId,
        int messageSize=0,
        Timestamp time = Timestamp.valueOf(msg.created.toLocalDateTime()),
        Long locationGroup = null
    ) {
            if (msg.offset == null) {
                sql.execute(
                    "INSERT INTO EVENTS(msg_key, content_type, type, created_utc, data, event_size, cluster_id, location_group) VALUES(?,?,?,?,?,?,?,?);",
                    msg.key, msg.contentType, msg.type, time, msg.data, messageSize, clusterId, locationGroup
                )
            } else {
                sql.execute(
                    "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id, location_group) VALUES(?,?,?,?,?,?,?,?,?);",
                    msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, messageSize, clusterId, locationGroup
                )
            }
    }

    void insertWithClusterAndTTL(
        long offset,
        String key,
        Long clusterId,
        LocalDateTime ttl
    ) {
        sql.execute(
            "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id, time_to_live) VALUES(?,?,?,?,?,?,?,?,?);",
            offset, key, "content-type", "type", Timestamp.valueOf(LocalDateTime.now()), "data", 1, clusterId, Timestamp.valueOf(ttl)
        )
    }

    @NamedVariant
    @Override
    Message message(Long offset, String type, String key, String contentType, ZonedDateTime created, String data) {
        new Message(
            type ?: "type",
            key ?: "key",
            contentType ?: "contentType",
            offset,
            created ?: time,
            data ?: "data"
        )
    }

    Optional<ClusterCacheEntry> cacheEntry(String location, List<Long> clusterIds, LocalDateTime expiry = LocalDateTime.now().plusMinutes(1), boolean valid = true) {
        Optional.of(new ClusterCacheEntry(location, clusterIds, expiry, valid))
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

    void insertLocationGroupFor(String locationUuid, List<Long> locationGroups) {
        Connection connection = DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        Array groups = connection.createArrayOf("integer", locationGroups.toArray())
        sql.execute("INSERT INTO LOCATION_GROUPS(location_uuid, groups) VALUES (?, ?)", locationUuid, groups)
    }
}
