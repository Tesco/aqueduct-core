package com.tesco.aqueduct.pipe.storage

import spock.lang.Specification

import java.time.LocalDateTime

class ClusterCacheTest extends Specification {
    def "is cached if entry is valid and not expired"() {
        given:
        def entry = new ClusterCache("location", [], LocalDateTime.now().plusMinutes(1), true)

        expect:
        entry.isCached()
    }

    def "is not cached if entry is not valid"() {
        given:
        def entry = new ClusterCache("location", [], LocalDateTime.now().plusMinutes(1), false)

        expect:
        !entry.isCached()
    }

    def "is not cached if entry is expired"() {
        given:
        def entry = new ClusterCache("location", [], LocalDateTime.now().minusMinutes(1), true)

        expect:
        !entry.isCached()
    }

    def "is not cached if entry is expired and not valid"() {
        given:
        def entry = new ClusterCache("location", [], LocalDateTime.now().minusMinutes(1), false)

        expect:
        !entry.isCached()
    }
}
