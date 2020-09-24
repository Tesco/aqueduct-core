package com.tesco.aqueduct.pipe.codec

import spock.lang.Specification

class BrotliCodecSpec extends Specification {

    def "Encoded data is decoded correctly"() {
        given:
        def brotliCodec = new BrotliCodec(4, false)

        and:
        def inputData = someRichJson()
        def inputDataSize = inputData.getBytes().size()

        when: "encoded"
        def encodedBytes = brotliCodec.encode(inputData.bytes)

        then:
        new String(brotliCodec.decode(encodedBytes)) == inputData

        and:
        encodedBytes.size() < inputDataSize
    }

    def "Pipe codec exception thrown when compression format is not Brotli"() {
        given:
        def brotliCodec = new BrotliCodec(4, false)

        and:
        def inputData = "Som non brotli codec data"

        when:
        brotliCodec.decode(inputData.bytes)

        then:
        thrown(PipeCodecException)
    }

    def "Codec type is Brotli"() {
        expect:
        new BrotliCodec(4, false).getHeaderType() == "br"
    }

    String someRichJson() {
        """
            [
              {
                "type": "type1",
                "key": "A",
                "contentType": "content-type",
                "offset": "1",
                "created": "2000-12-01T10:00:00Z",
                "data": "{\"type\":\"someType\",\"id\":\"someId\",\"data\":[{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"0.0\",\"key4\":\"ASD\",\"UUID-random\":\"2aa2ea7f-1066-4e91-a570-8a3210e68922\",\"key5\":[\"1:value\",\"2:value\",\"3:value\"],\"date\":\"2018-09-11T00:00:00+01:00\",\"emptyKey\":[]}]}"
              },
              {
                "type": "type2",
                "key": "D",
                "contentType": "content-type",
                "offset": "4",
                "created": "2000-12-01T10:00:00Z",
                "data": "{\"id\":\"someId\",\"arraykey\":[\"2231bb94-05c9-4914-a37e-55ce1907fd02\"],\"number\":\"8899\",\"arrayNum\":[\"13\",\"6\"],\"dateTime1\":\"2020-06-08T00:00:00+01:00\",\"dateTime2\":\"2020-08-25T23:59:59Z\",\"name\":\"someName\",\"description\":\"someDescription\",\"description1\":\"SomeMoreDescription\",\"description2\":\"someBigDescription\",\"num2\":223456,\"object\":{\"type\":\"someObjectType\",\"id\":\"someObjectId\",\"subObject\":[{\"type\":\"someSubObjectType\",\"id\":\"9\",\"subObjectKey\":{\"type\":\"someSubObjectKeyType\",\"size\":0098,\"arrayNum\":[22345,556778,0,0,9,324,24,55435,55534534,34543634634534634]}}]}}"
              },
              {
                "type": "type2",
                "key": "D",
                "contentType": "content-type",
                "offset": "5",
                "created": "2000-12-01T10:00:00Z",
                "data": "łąś»«·§≠²³€²≠³½ßśćąż"
              }
            ]
        """
    }
}
