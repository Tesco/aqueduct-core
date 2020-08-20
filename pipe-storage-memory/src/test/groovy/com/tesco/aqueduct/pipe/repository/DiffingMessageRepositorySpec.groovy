package com.tesco.aqueduct.pipe.repository

import com.tesco.aqueduct.pipe.codec.CodecType
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.time.Duration
import java.time.ZonedDateTime

class DiffingMessageRepositorySpec extends Specification {

    def "data with the same key existing before returns patch if smaller than data"(){
        given: "json patching repository"
        def repository = new DiffingMessageRepository(true, CodecType.BROTLI)
        and: "boring long json, with one having extra element in array"
        def events = 20
        def firstJson = [ events:
            (1..events).collect {[ eventId: it ] }
        ]
        def secondJson = [ events:
            (1..(events+1)).collect {[ eventId: it ] }
        ]

        when: "messages with same keys are stored"
        repository.put(message(1, "key", firstJson))
        repository.put(message(2, "key", secondJson))

        then: "second message has a patch assigned"
        new String(repository.get(2).getPatch().patch) ==
            '[{"op":"add","path":"/events/-","value":{"eventId":21}}]'
    }

    BinaryMessageWithPatch message(long offset, String key, Map json) {
        def data = JsonOutput.toJson(json).bytes

        new BinaryMessageWithPatch(
            "type",
            key,
            "application/json",
            offset,
            ZonedDateTime.now(),
            data,
            CodecType.NONE,
            data.size(),
            null
        )
    }

    /**
     * Used with Very Big File on my machine, that is in jsonl format:
     * <pre>
{"msg_offset":1,"msg_key":"x","content_type":"application/json","type":"type","created_utc":"2019-06-19T10:48:31.209Z","data":"{"some":"data"}
{"msg_offset":2,"msg_key":"x","content_type":"application/json","type":"type","created_utc":"2019-06-19T10:48:31.209Z","data":"{"some":"data"}
     * </pre>
     */
    @IgnoreIf({ !new File("../events.json").exists()})
    def "Read big stuff from disk"(){
        given:
        def file = "../events.json" as File
        def slurper = new JsonSlurper()
        def repository = new DiffingMessageRepository(true, CodecType.BROTLI)

        when:
        int i = 0

        def start = System.nanoTime()
        def prev = start
        file.eachLine {
            i++
            def json = slurper.parseText(it)
            def msg = new BinaryMessageWithPatch(
                json.type,
                json.msg_key,
                json.content_type,
                json.msg_offset as Long,
                ZonedDateTime.parse(json.created_utc),
                json.data?.bytes,
                CodecType.NONE,
                json.size,
                null
            )

            if(i % 100_000 == 0) {
                def current = System.nanoTime()
                def diff = current - prev
                prev = current
                println(Duration.ofNanos(diff).toMillis() / 1000 + " s " + i)
            }

            repository.put(msg)
        }

        def end = System.nanoTime()
        def taken = Duration.ofNanos(end - start).toMillis() / 1000

        println "Inserted $i messages in $taken seconds"

        Thread.sleep(1000*60*10) // wait so I can do a heap dump

        then:
        noExceptionThrown()
    }
}
