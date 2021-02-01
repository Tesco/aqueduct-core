package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.JsonHelper
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import groovy.sql.Sql
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.sqlite.SQLiteDataSource
import spock.lang.Specification

import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.function.Supplier

class TimedDistributedStorageIntegrationSpec extends Specification {
    static final def CONNECTION_URL = "jdbc:sqlite:aqueduct-pipe.db"
    static final def LIMIT = 1000
    static final long BATCH_SIZE = 1000

    def meterRegistry = Mock(MeterRegistry)

    ZonedDateTime currentUTCTime() {
        ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC"))
    }

    def message(long offset) {
        def now = currentUTCTime()
        def messageForSizing = new Message(
            "some-key",
            "some-type",
            "text/plain",
            offset,
            now,
            "some-data"
        )

        return new Message(
            "some-key",
            "some-type",
            "text/plain",
            offset,
            now,
            "some-data",
            JsonHelper.toJson(messageForSizing).length()
        )
    }

    def successfulDataSource() {
        def dataSource = new SQLiteDataSource()
        dataSource.setUrl(CONNECTION_URL)
        return dataSource
    }

    def setup() {
        def sql = Sql.newInstance(CONNECTION_URL)
        sql.execute("DROP TABLE IF EXISTS EVENT;")

        def timer = Mock(Timer)
        meterRegistry.timer(_ as String) >> timer
        timer.record(_ as Supplier) >> { return it.first().get() }
        timer.record(_ as Runnable) >> { return it.first().run() }
    }

    def 'we can write a single message to the database via TimedMessageStorage'() {
        def offset = 1023L

        given: 'a message'
        Message message = message(offset)

        when: 'we use the SQLiteStorage to create a connection'
        SQLiteStorage sqliteStorage = new SQLiteStorage(successfulDataSource(), LIMIT, 10, BATCH_SIZE)

        and: 'we use the TimedMessageStorage class to wrap sqlLite'
        TimedDistributedStorage storage = new TimedDistributedStorage(sqliteStorage, meterRegistry)

        and: 'store the message to the database'
        storage.write(message)

        then: 'the message is stored with no errors'
        notThrown(Exception)
    }

    def 'we can write multiple messages to the database via TimeMessageStorage'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2)]

        and: 'a database table exists to be written to'
        def sql = Sql.newInstance(CONNECTION_URL)

        and: 'a data store controller exists'
        def sqliteStorage = new SQLiteStorage(successfulDataSource(), LIMIT, 10, BATCH_SIZE)

        when: 'we use the TimedMessageStorage class to wrap sqlLite'
        TimedDistributedStorage storage = new TimedDistributedStorage(sqliteStorage, meterRegistry)

        and: 'these messages are passed to the data store controller'
        storage.write(messages)

        then: 'all messages are written to the data store'
        def size = 0
        sql.query("SELECT COUNT(*) FROM EVENT", {
            it.next()
            size = it.getInt(1)
        })

        size == 2
    }

    def 'we can read from the database via TimedMessageStorage'() {
        def offset = 1023L

        given: 'a message'
        Message message = message(offset)

        when: 'we use the sqliteStorage to create a connection'
        SQLiteStorage sqliteStorage = new SQLiteStorage(successfulDataSource(), LIMIT, 10, BATCH_SIZE)

        and: 'we use the TimedMessageStorage class to wrap sqlLite'
        TimedDistributedStorage storage = new TimedDistributedStorage(sqliteStorage, meterRegistry)

        and: 'store the message to the database'
        storage.write(message)

        and: 'we retrieve the message from the database'
        MessageResults messageResults = storage.read(null, offset, "locationUuid")
        Message retrievedMessage = messageResults.messages.get(0)

        then: 'the message retrieved should be what we saved'
        notThrown(Exception)
        message == retrievedMessage
    }
}
