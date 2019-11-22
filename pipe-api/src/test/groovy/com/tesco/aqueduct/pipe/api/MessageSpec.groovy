package com.tesco.aqueduct.pipe.api

import spock.lang.Specification

import java.time.ZoneOffset
import java.time.ZonedDateTime

class MessageSpec extends Specification {

    def "test Message serialization format"(){
        given:
        def msg = new Message(
            "type1",
            "key1",
            "application/json",
            123,
            ZonedDateTime.of(2018, 11, 4, 8, 57, 45, 0, ZoneOffset.UTC),
            "data1"
        )

        when:
        def json = JsonHelper.toJson(msg)

        then:
        // Offset is represented as string even though it is a number
        json == '''
            {
               "type":"type1",
               "key":"key1",
               "contentType":"application/json",
               "offset": "123",
               "created": "2018-11-04T08:57:45Z",
               "data":"data1"
            }
        '''.replaceAll(/\s|\n/, "")
    }

    def "Message deserialisation format"() {
        given:
        def json = """
            {
               "type":"type1",
               "key":"key1",
               "contentType":"application/json",
               "offset": $jsonOffset,
               "created": "2018-11-04T08:57:45Z",
               "data":"data1"
            }
        """

        when:
        def actualMsg = JsonHelper.fromJson(json, Message.class)

        then:
        actualMsg == new Message(
            "type1",
            "key1",
            "application/json",
            offset,
            ZonedDateTime.of(2018, 11, 4, 8, 57, 45, 0, ZoneOffset.UTC),
            "data1"
        )

        where:
        jsonOffset | offset | description
        '123'      | 123    | 'Can parse offset as number'
        '"124"'    | 124    | 'Can parse offset as String'
    }


    def "deserializing minimal message"() {
        given:
        def msg = '''
            {
              "type": "type1",
              "key": "x",
              "offset": 1,
              "created": "2018-10-01T13:45:00Z"
            }
        '''

        when:
        def m = JsonHelper.fromJson(msg, Message.class)

        then:
        m.type == 'type1'
        m.key == 'x'
        m.offset == 1
        m.created == ZonedDateTime.parse("2018-10-01T13:45:00Z")
    }
}
