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

import javax.sql.DataSource
import java.sql.*
import java.time.ZonedDateTime

class PostgresqlStorageIntegrationSpec extends StorageSpec {

    // Starts real PostgreSQL database, takes some time to create it and clean it up.
    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    PostgresqlStorage storage
    DataSource dataSource

    private static final long CLUSTER_A = 1L
    private static final long CLUSTER_B = 2L
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
        DROP TABLE IF EXISTS CLUSTERS;
          
        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1
        );
        
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');
        """)

        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize, 0, 1, 1)
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
        def messageResults = storage.read(["some_type"], 0, ["clusterId"])

        then: "pipe state is up to date"
        messageResults.pipeState == PipeState.UP_TO_DATE
    }

    def "get messages for given type and clusters"() {
        given: "there is postgres storage"
        def limit = 1
        def dataSourceWithMockedConnection = Mock(DataSource)
        def postgresStorage = new PostgresqlStorage(dataSourceWithMockedConnection, limit, 0, batchSize, 0, 1, 1)

        and: "a mock connection is provided when requested"
        def connection = Mock(Connection)
        dataSourceWithMockedConnection.getConnection() >> connection

        and: "a connection returns a prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        preparedStatement.executeQuery() >> Mock(ResultSet)
        connection.prepareStatement(_ as String) >> preparedStatement

        when: "requesting messages specifying a type key and a cluster id"
        postgresStorage.read(["some_type"], 0, ["clusterId"])

        then: "a query is created that contain given type and cluster including default cluster in the where clause"
        2 * preparedStatement.setLong(_, 0)
        1 * preparedStatement.setString(_, "some_type")
        1 * preparedStatement.setString(_, "clusterId,NONE")
    }

    def "the messages returned are no larger than the maximum batch size when reading without a type"() {
        given: "there are messages with unique keys"
        def msg1 = message(key: "x")
        def msg2 = message(key: "y")
        def msg3 = message(key: "z")

        and: "the size of each message is set so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "they are inserted into the integrated database"
        insert(msg1, messageSize)
        insert(msg2, messageSize)
        insert(msg3, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read([], 0, ["clusterId"])

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
        insert(msg1, messageSize)
        insert(msg2, messageSize)
        insert(msg3, messageSize)

        when: "reading from the database"
        MessageResults result = storage.read(["type-1"], 0, ["clusterId"])

        then: "messages that are returned are no larger than the maximum batch size"
        result.messages.size() == 2
    }

    def "retry-after is non-zero if the pipe has no more data at specified offset"() {
        given: "I have some records in the integrated database"
        insert(message(key: "z"))
        insert(message(key: "y"))
        insert(message(key: "x"))

        when:
        MessageResults result = storage.read([], 4, ["clusterId"])

        then:
        result.retryAfterSeconds > 0
        result.messages.isEmpty()
    }

    def "retry-after is non-zero if the pipe has no data"() {
        given: "I have no records in the integrated database"

        when:
        MessageResults result = storage.read([], 0,["clusterId"])

        then:
        result.retryAfterSeconds > 0
        result.messages.isEmpty()
    }

    def 'All duplicate messages are compacted for whole data store'() {
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")

        and: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(3, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)

        when: 'compaction is run on the whole data store'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults result = storage.read(null, 0, ["cluster1"])
        List<Message> retrievedMessages = result.messages

        then: 'duplicate messages are deleted'
        retrievedMessages.size() == 2

        and: 'the correct compacted message list is returned in the message results'
        result.messages*.offset*.intValue() == [2, 3]
        result.messages*.key == ["B", "A"]
    }

    def 'Messages with the same key but different clusters are not compacted'() {
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'an existing data store with 2 messages with same key but different clusters'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)

        when: 'compaction is run on the whole data store'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults result = storage.read(null, 0, ["cluster1", "cluster2"])
        List<Message> retrievedMessages = result.messages

        then:
        retrievedMessages.size() == 2
        and: 'the correct compacted message list is returned in the message results'
        result.messages*.offset*.intValue() == [1, 2]
        result.messages*.key == ["A", "A"]
    }

    def 'Duplicate messages are not compacted when published after the threshold'() {
        given: 'a stored cluster'
        Long cluster1 = insertCluster("cluster1")

        and: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(3, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(4, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster1)

        when: 'compaction is run up to the timestamp of offset 1'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, ["cluster1"])

        then: 'duplicate messages are not deleted as they are beyond the threshold'
        messageResults.messages.size() == 4
        messageResults.messages*.offset*.intValue() == [1, 2, 3, 4]
        messageResults.messages*.key == ["A", "A", "B", "A"]
    }

    def 'All duplicate messages are compacted to a given offset, complex case'() {
        given: 'an existing data store with duplicate messages for the same key'
        insert(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(6, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(7, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))
        insert(message(8, "type", "D", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"))

        when: 'compaction is run up to the timestamp of offset 4'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, ["cluster1"])

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 7
        messageResults.messages*.offset*.intValue() == [1, 2, 4, 5, 6, 7, 8]
        messageResults.messages*.key == ["A", "B", "C", "A", "B", "B", "D"]
    }

    def 'All duplicate messages are compacted to a given offset per cluster, complex case'() {
        given: 'a stored cluster'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'an existing data store with duplicate messages for the same key'
        insertWithCluster(message(1, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(3, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(4, "type", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(5, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(6, "type", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(7, "type", "B", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(8, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(9, "type", "A", "content-type", ZonedDateTime.parse("2000-12-03T10:00:00Z"), "data"), cluster2)

        when: 'compaction is run up to the timestamp of offset 4'
        storage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = storage.read(null, 1, ["cluster1", "cluster2"])

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 7
        messageResults.messages*.offset*.intValue() == [2, 4, 5, 6, 7, 8, 9]
        messageResults.messages*.key == ["A", "A", "B", "B", "B", "A", "A"]
    }

    @Unroll
    def 'Global latest offset is returned'() {
        given: 'an existing data store with two different types of messages'
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read([type], 0, ["clusterId"])

        then: 'global latest offset is type and clusterId independent'
        messageResults.globalLatestOffset == OptionalLong.of(3)

        where:
        type << ["type1", "type2", "type3"]
    }

    def 'pipe should return all messages when no types are provided and all messages have default cluster'(){
        given: 'some messages are stored with default cluster'
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading with no types'
        def messageResults = storage.read([], 0, ["clusterId"])

        then: 'all messages from the storage are returned regardless the types'
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
    }

    def 'pipe should return messages for given clusters and no type'() {
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'some messages are stored with clusters'
        insertWithCluster(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)

        when: 'reading with no types but cluster provided'
        def messageResults = storage.read([], 0, ["cluster1"])

        then: 'messages belonging to cluster1 are returned'
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["A", "C"]
        messageResults.messages*.offset*.intValue() == [1, 3]
    }

    def 'pipe should return messages for given clusters and default cluster'() {
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'some messages are stored with clusters'
        insertWithCluster(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)

        and: 'some messages without cluster'
        insert(message(4, "type1", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type2", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading with no types but cluster provided'
        def messageResults = storage.read([], 0, ["cluster1"])

        then: 'messages belonging to cluster1 and default cluster are returned'
        messageResults.messages.size() == 4
        messageResults.messages*.key == ["A", "C", "D", "E"]
        messageResults.messages*.offset*.intValue() == [1, 3, 4, 5]
    }

    def 'pipe should return relevant messages when types and cluster are provided'(){
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'some messages are stored'
        insertWithCluster(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(6, "type3", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)

        when: 'reading with no types but cluster provided'
        def messageResults = storage.read(["type2", "type3"], 0, ["cluster1"])

        then: 'messages belonging to cluster1 are returned'
        messageResults.messages.size() == 2
        messageResults.messages*.key == ["D", "F"]
        messageResults.messages*.offset*.intValue() == [4, 6]
    }

    def 'pipe should return relevant messages for given types and cluster including default cluster'() {
        given: 'some clusters are stored'
        Long cluster1 = insertCluster("cluster1")
        Long cluster2 = insertCluster("cluster2")

        and: 'some messages are stored'
        insertWithCluster(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster2)
        insertWithCluster(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)
        insertWithCluster(message(6, "type3", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"), cluster1)

        and: 'some messages without cluster'
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(8, "type2", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading with no types but cluster provided'
        def messageResults = storage.read(["type2", "type3"], 0, ["cluster1"])

        then: 'messages belonging to cluster1 are returned'
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["D", "F", "H"]
        messageResults.messages*.offset*.intValue() == [4, 6, 8]
    }

    def "pipe should return messages if available from the given offset instead of empty set"() {
        given: "there is postgres storage"
        def limit = 3
        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize, 0, 1, 1)

        and: 'an existing data store with two different types of messages'
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(5, "type2", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type2", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(9, "type1", "I", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read(["type1"], 0, ["cluster1"])

        then: 'messages are provided for the given type'
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
        messageResults.globalLatestOffset == OptionalLong.of(9)

        when: "read again from further offset"
        messageResults = storage.read(["type1"], 4, [])

        then: "we should still get relevant messages back even if they are further down from the given offset"
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["G", "H", "I"]
        messageResults.messages*.offset*.intValue() == [7, 8, 9]
        messageResults.globalLatestOffset == OptionalLong.of(9)
    }

    def "getMessageCountByType should return the count of messages by type"() {
        given: "there is postgres storage"
        def limit = 3
        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize, 0, 1, 1)

        and: 'an existing data store with two different types of messages'
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
        storage.runVisibilityCheck()
        Map<String, Long> result = storage.getMessageCountByType(dataSource.connection)

        then: "the correct count is returned"
        result.get("type1") == 6
        result.get("type2") == 3

        noExceptionThrown()
    }

    @Unroll
    def "when messages have out of order created_utc, we read up to the message with minimum offset outside the limit"() {
        given:
        storage = new PostgresqlStorage(dataSource, limit, retryAfter, batchSize, 0, 1, 1)
        storage.currentTimestamp = "TO_TIMESTAMP( '2000-12-01 10:00:01', 'YYYY-MM-DD HH:MI:SS' )"

        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(2, "type1", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(3, "type1", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        // messages out of order
        insert(message(4, "type1", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))
        insert(message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(6, "type1", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))
        insert(message(7, "type1", "G", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))
        insert(message(8, "type1", "H", "content-type", ZonedDateTime.parse("2000-12-01T10:00:01Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read(types, 1, [])

        then: 'messages are provided for the given type'
        messageResults.messages.size() == 3
        messageResults.messages*.key == ["A", "B", "C"]
        messageResults.messages*.offset*.intValue() == [1, 2, 3]
        messageResults.globalLatestOffset == OptionalLong.of(3)

        where:
        types << [ [], ["type1"] ]
    }

    @Unroll
    def "read up to the last message in the pipe when all are in the visibility window"() {
        given:
        insert(message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data"))

        when: 'reading all messages'
        def messageResults = storage.read(types, 1, [])

        then: 'messages are provided for the given type'
        messageResults.messages.size() == 1
        messageResults.messages*.key == ["A"]
        messageResults.messages*.offset*.intValue() == [1]
        messageResults.globalLatestOffset == OptionalLong.of(1)

        where:
        types << [ [], ["type1"] ]
    }

    @Override
    void insert(Message msg, int maxMessageSize=0, def time = Timestamp.valueOf(msg.created.toLocalDateTime()) ) {

        if (msg.offset == null) {
            sql.execute(
                "INSERT INTO EVENTS(msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?);",
                msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize
            )
        } else {
            sql.execute(
                "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?,?);",
                msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize
            )
        }
    }

    void insertWithCluster(Message msg, Long clusterId, def time = Timestamp.valueOf(msg.created.toLocalDateTime()), int maxMessageSize=0) {
        sql.execute(
            "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
            msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize, clusterId
        )
    }

    Long insertCluster(String clusterUuid){
        sql.executeInsert("INSERT INTO CLUSTERS(cluster_uuid) VALUES (?);", [clusterUuid]).first()[0]
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
}
