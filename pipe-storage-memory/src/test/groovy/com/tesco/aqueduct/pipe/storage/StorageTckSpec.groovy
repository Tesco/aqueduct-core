package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.DistributedStorage
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import com.tesco.aqueduct.pipe.api.PipeState
import groovy.transform.NamedVariant
import spock.lang.Specification

import java.time.Duration
import java.time.ZonedDateTime

abstract class StorageTckSpec extends Specification {

    //TODO: distributed storage is wrong name, it's just storage when it can read and write
    // CentralStorage should have extra "publish" method which is not write as it does not take offset
    // So it should use separate type as parameter to distinguish new message vs message with offset
    abstract DistributedStorage getStorage()

    ZonedDateTime date = ZonedDateTime.parse("2020-12-23T21:45:59.123Z");

    //TODO: check for null data

    def "Message written to storage can be read from storage"() {
        given:
        def message = message(offset:  123)
        storage.write(message)

        when:
        def result = storage.read([], message.offset, [])

        then:
        result.messages == [message]
    }

    def "Many messages written to storage can be read from storage starting from given offset, inclusive"() {
        given:
        storage.write([
            message(offset:  2),
            message(offset:  3),
            message(offset:  4),
            message(offset:  5),
            message(offset:  8),
            message(offset:  10),
        ])

        when:
        def result = storage.read([], 3, [])

        then:
        result.messages*.offset == [3,4,5,8,10]
    }

    def "If types are provided they are used to filter messages"() {
        given:
        storage.write([
            message(offset:  2, type: "a"), // xxx before start offset
            message(offset:  3, type: "b"),
            message(offset:  4, type: "a"), // <--
            message(offset:  5, type: "c"), // <--
            message(offset:  8, type: "b"),
            message(offset:  10, type: "c"),// <--
        ])

        when:
        def result = storage.read(["a", "c"], 3, [])

        then:
        result.messages*.type == ["a","c","c"]
        result.messages*.offset == [4,5,10]
    }

    def "If types are null or empty all messages are returned"() {
        given:
        storage.write([
            message(offset:  1, type: "a"),
            message(offset:  2, type: "b"),
            message(offset:  3, type: "a")
        ])

        when:
        def result1 = storage.read([], 0, [])
        def result2 = storage.read(null, 0, [])

        then:
        result1.messages*.offset == [1,2,3]
        result1 == result2
    }

    def "Storage state is by default unknown" (){
        expect:
        storage.getPipeState() == PipeState.UNKNOWN
    }

    def "Storage state can be set" (){
        when:
        storage.write(PipeState.UP_TO_DATE)

        then:
        storage.getPipeState() == PipeState.UP_TO_DATE
    }

    def "Write 1mln messages and read them back"(){
        given:
        // tested locally with 6mln without any issues, write time does not visibly increase
        def messageCount = 1_000_000

        def start = System.nanoTime()
        def prev = start

        measure("Inserted in") {
            messageCount.times {
                storage.write(message(offset: it))
                if (it % 100_000 == 0) {
                    def current = System.nanoTime()
                    def diff = current - prev
                    prev = current
                    println(Duration.ofNanos(diff).toMillis() / 1000 + " s " + it)
                }

            }
            println "Inserted $messageCount messages"
        }

        when:
        measure("Read in") {
            MessageResults results = storage.read([], 0, [])
            int count = results.messages.size()
            while (!results.messages.empty) {
                results = storage.read([], results.messages.last().offset+1, [])
                count += results.messages.size()
            }
            println "Read $count messages"
        }


        then:
        noExceptionThrown()
    }

    private static <T> T measure(String msg, Closure<T> c) {
        def start = System.nanoTime()
        c.call()
        def end = System.nanoTime()
        def taken = Duration.ofNanos(end - start).toMillis() / 1000
        println "$msg $taken seconds"
    }

    @NamedVariant
    Message message(Long offset, String type, String key, String contentType, ZonedDateTime created, String data) {
        assert offset >= 0

        new Message(
            type ?: "type",
            key ?: "key",
            contentType ?: "contentType",
            offset,
            created ?: date.plusNanos(offset),
            data ?: "data"
        )
    }
}
