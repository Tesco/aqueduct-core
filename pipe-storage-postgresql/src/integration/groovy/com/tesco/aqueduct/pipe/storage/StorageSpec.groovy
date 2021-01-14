package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.Reader
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

abstract class StorageSpec extends Specification {

    static ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
    static limit = 1000

    @Shared
    def msg1 = message(offset: 102, key:"x")

    @Shared
    def msg2 = message(offset: 107, key:"y")

    abstract Reader getStorage();
    abstract void insertWithCluster(Message msg, Long clusterId = 1L);
    abstract void insertLocationInCache(String locationUuid, List<Long> clusterIds, def expiry = Timestamp.valueOf(LocalDateTime.now() + TimeUnit.MINUTES.toMillis(1)), boolean valid = true)

    def "can persist messages without offset"() {
        given:
        insertLocationInCache("locationUuid", [1L])
        insertWithCluster(message(offset: null))
        insertWithCluster(message(offset: null))

        when:
        List<Message> messages = storage.read(null, 0, "locationUuid").messages

        then:
        messages*.offset == [1,2]
    }

    // test for the test insert method
    def "can persist message with offset if set"() {
        given:
        insertWithCluster(msg1)
        insertWithCluster(msg2)

        when:
        List<Message> messages = storage.read(null, 0, "locationUuid").messages

        then:
        messages*.offset == [102,107]
    }

    def "can get the message we inserted"() {
        given: "A message in database"
        def msg = message(offset: 1)
        insertWithCluster(msg)

        when: "When we read"
        List<Message> messages = storage.read(null, 0, "locationUuid").messages

        then: "We get exactly the message we send"
        messages == [msg]
    }

    def "number of entities returned respects limit"() {
        given: "more messages in database than the limit"
        (limit * 2).times {
            insertWithCluster(message(key: "$it"))
        }

        when:
        def messages = storage.read(null, 0, "").messages.toList()

        then:
        messages.size() == limit
    }


    @Unroll
    def "filter by types #types"() {
        given:
        insertWithCluster(message(type: "type-v1"))
        insertWithCluster(message(type: "type-v2"))
        insertWithCluster(message(type: "type-v3"))

        when:
        List<Message> messages = storage.read(types, 0, "locationUuid").messages

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
        insertWithCluster(msg1)
        insertWithCluster(msg2)

        then:
        storage.read(null, offset, "locationUuid").messages == result

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
        insertWithCluster(message(key:"x"))
        insertWithCluster(message(key:"x"))
        insertWithCluster(message(key:"x"))

        then:
        storage.read(null, 0, "locationUuid").messages.size() == 3
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
