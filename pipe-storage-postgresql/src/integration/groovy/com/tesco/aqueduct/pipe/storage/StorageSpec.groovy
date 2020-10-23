package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.Reader
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

abstract class StorageSpec extends Specification {

    static ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
    static limit = 1000

    @Shared
    def msg1 = message(offset: 102, key:"x", )

    @Shared
    def msg2 = message(offset: 107, key:"y")

    abstract Reader getStorage();
    abstract void insert(Message msg);

    def "can persist messages without offset"() {
        given:
        insert(message(offset: null))
        insert(message(offset: null))

        when:
        List<Message> messages = storage.read(null, 0, []).messages

        then:
        messages*.offset == [1,2]
    }

    // test for the test insert method
    def "can persist message with offset if set"() {
        given:
        insert(msg1)
        insert(msg2)

        when:
        List<Message> messages = storage.read(null, 0, []).messages

        then:
        messages*.offset == [102,107]
    }

    def "can get the message we inserted"() {
        given: "A message in database"
        def msg = message(offset: 1)
        insert(msg)

        when: "When we read"
        List<Message> messages = storage.read(null, 0, []).messages

        then: "We get exactly the message we send"
        messages == [msg]
    }

    def "number of entities returned respects limit"() {
        given: "more messages in database than the limit"
        (limit * 2).times {
            insert(message(key: "$it"))
        }

        when:
        def messages = storage.read(null, 0, []).messages.toList()

        then:
        messages.size() == limit
    }


    @Unroll
    def "filter by types #types"() {
        given:
        insert(message(type: "type-v1"))
        insert(message(type: "type-v2"))
        insert(message(type: "type-v3"))

        when:
        List<Message> messages = storage.read(types, 0, []).messages

        then:
        messages.size() == resultsSize

        where:
        types                             | resultsSize
        ["typeX"]                         | 0
        ["type-v1"]                       | 1
        ["type-v2", "type-v3"]            | 2
        ["type-v1", "type-v3"]            | 2
        ["type-v1", "type-v2", "type-v3"] | 3
        []                                | 3
        null                              | 3

    }

    @Unroll
    def "basic message behaviour - #rule"() {
        when:
        insert(msg1)
        insert(msg2)

        then:
        storage.read(null, offset, []).messages == result

        where:
        offset          | result       | rule
        msg2.offset + 1 | []           | "returns empty if offset after data"
        msg1.offset - 1 | [msg1, msg2] | "returns both messages after a lower offset"
        msg1.offset     | [msg1, msg2] | "returns both messages after or equal to a lower offset"
        msg1.offset + 1 | [msg2]       | "returns the messages after an offset"
        msg2.offset     | [msg2]       | "returns the messages after or equal to an offset"
    }

    def "compaction - same not immediately compacted"() {
        when:
        insert(message(key:"x"))
        insert(message(key:"x"))
        insert(message(key:"x"))

        then:
        storage.read(null, 0, []).messages.size() == 3
    }

    abstract Message message(
        Long offset,
        String type,
        String key,
        String contentType,
        ZonedDateTime created,
        String data
    );
}
