package com.tesco.aqueduct.pipe.repository

import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.pipe.codec.Codec
import com.tesco.aqueduct.pipe.codec.GzipCodec
import spock.lang.Shared
import spock.lang.Specification

import java.nio.charset.Charset

class CodecSpec extends Specification {

    static Charset UTF8 = Charset.forName("UTF-8")
    @Shared List<Codec> codecs = [new GzipCodec(), new BrotliCodec()]

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
        def encoded = codec.encode(input.getBytes(UTF8))

        then:
        encoded.size() * 2 < input.bytes.size()
        println("Codec: $codec.type ratio: ${encoded.size() / input.bytes.size()}")

        where:
        codec << codecs
    }

    def "Encoded string can be decoded to equal string "() {
        when:
        def encoded = codec.encode(input.getBytes(UTF8))

        then:
        input == new String(codec.decode(encoded), UTF8)

        where:
        codec << codecs
    }
}
