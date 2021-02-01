package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.*
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET

class SQLiteStorageSpec extends Specification {

    static final def connectionUrl = "jdbc:sqlite:aqueduct-pipe.db"
    static final def limit = 1000

    long batchSize = 1000

    def message(long offset, String type) {
        def timeNow = ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC"))
        return new Message(
            type,
            "some-key",
            "text/plain",
            offset,
            timeNow,
            null
        )
    }

    def dataSource
    @Shared
    SQLiteStorage sqliteStorage

    def setup() {
        dataSource = Mock(DataSource)
        dataSource.getConnection() >> {DriverManager.getConnection(connectionUrl)}
        sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)
        //make sure everything is clear before starting
        sqliteStorage.deleteAll()
    }

    def message(long offset) {
        return message(offset, "some-type")
    }

    def 'throws an exception if a problem with the database arises when reading messages'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            {throw new SQLException()}
        sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)

        when: 'messages are requested to be read from a given offset'
        sqliteStorage.read([], 0, "abc")

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing messages'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            {throw new SQLException()}
        sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)

        when: 'the latest offset is requested'
        sqliteStorage.write(message(1))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing latest offset'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            {throw new SQLException()}
        sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)

        when: 'the latest offset is written'
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(100)))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if no offset value provided when writing latest offset'() {
        given: 'a data store controller and sqlite storage exists'
        def dataSource = Mock(DataSource)
        dataSource.getConnection() >>> [
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl)
        ]

        def sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)

        when: 'the latest offset is written with empty value'
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.empty()))

        then: 'no such element exception is thrown'
        thrown(NoSuchElementException)
    }

    def 'retry read time limit should be activated only when the amount of received messages is 0'() {
        given: 'a data store controller'
        def retryAfter = 10
        def sqliteStorage = new SQLiteStorage(dataSource, testLimit, retryAfter, batchSize)

        when: 'the retry after is calculated'
        def actualRetryAfter = sqliteStorage.calculateRetryAfter(messageCount)

        then: 'the calculated retry after is 0 if more than 0 messages were returned'
        actualRetryAfter == expectedRetryAfter

        where:
        testLimit | messageCount | expectedRetryAfter
        100       | 50           | 0
        100       | 0            | 10
        100       | 99           | 0
        99        | 98           | 0
        99        | 0            | 10
        100       | 101          | 0
        101       | 102          | 0
        100       | 100          | 0
        99        | 100          | 0
        0         | 1            | 0
    }

    def 'concurrent write of global latest offset doesnt cause inconsistencies' () {
        given: 'sqlite storage with a message and offsets loaded'
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)
        sqliteStorage.write(message(1L))
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(1L)))
        sqliteStorage.write(PipeState.UP_TO_DATE)

        when: 'read is called and write happens concurrently'
        ExecutorService pool = Executors.newFixedThreadPool(1)
        pool.execute{
            //sleep just long enough for the offset write to run during the read
            sleep 5
            sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(10L)))
        }
        MessageResults messageResults = sqliteStorage.read(["some-type"],0, "na")

        then: 'global offset that is read is not the concurrently written one, but a consistent one'
        messageResults.globalLatestOffset.asLong == 1L
    }

    def "Illegal argument error when pipe entity is null"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write((PipeEntity)null)

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when pipe entity data is null"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity(null, null, null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when messages within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity([], null, null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when messages and offsets within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity([], [], null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when only offsets within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity(null, [], null))

        then:
        thrown(IllegalArgumentException)
    }

    def "running management tasks attempt vacuum and checkpoint onto sqlite storage"() {
        given: "mock datasource"
        def dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        def statement = Mock(PreparedStatement)

        and: "data source giving out connection on demand"
        dataSource.getConnection() >>> [
            // first three calls are for setting up database schema
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            connection,
            connection
        ]

        and:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when: "tuning is invoked"
        sqliteStorage.runMaintenanceTasks()

        then: "shrink memory attempted"
        1 * connection.prepareStatement(SQLiteQueries.SHRINK_MEMORY) >> statement
        1 * statement.execute()

        and: "vacuum is attempted"
        1 * connection.prepareStatement(SQLiteQueries.VACUUM_DB) >> statement
        1 * statement.execute()
        1 * statement.getUpdateCount() >> 0

        and: "checkpoint is attempted"
        1 * connection.prepareStatement(SQLiteQueries.CHECKPOINT_DB) >> statement
        1 * statement.execute()
    }
}
