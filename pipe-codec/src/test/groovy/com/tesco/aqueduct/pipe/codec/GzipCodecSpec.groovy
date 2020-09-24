package com.tesco.aqueduct.pipe.codec


import spock.lang.Specification

class GzipCodecSpec extends Specification {
    Codec codec = new GzipCodec(-1, false)

    def input = """
        Some example text that will be encoded by codec
        It has to be long enough otherwise it might not be get smaller once gzipped
        Also some duplication helps:
        duplicate1 duplicate2 duplicate3 duplicate4 duplicate5 duplicate6 duplicate7
        duplicate1 duplicate2 duplicate3 duplicate4 duplicate5 duplicate6 duplicate7
        duplicate1 duplicate2 duplicate3 duplicate4 duplicate5 duplicate6 duplicate7
        etc. the more duplicates the merrier
        Also some characters to check encoding łąś»«·§≠²³€²≠³½ßśćąż
    """

    def "Encoded input is smaller than original"() {
        when:
        def encoded = codec.encode(input.bytes)

        then:
        encoded.size() * 2 < input.bytes.size()
    }

    def "Encoded string can be decoded to equal string "() {
        when:
        def encoded = codec.encode(input.bytes)

        then:
        input == new String(codec.decode(encoded))
    }

    def "Pipe codec exception when inout cannot be decoded"() {
        when:
        codec.decode("some non encoded input".bytes)

        then:
        thrown(PipeCodecException)
    }

    def "Codec type is gzip"() {
        expect:
        codec.getHeaderType() == "gzip"
    }
}
