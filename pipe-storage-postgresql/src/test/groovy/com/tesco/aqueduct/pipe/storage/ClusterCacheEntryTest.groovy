package com.tesco.aqueduct.pipe.storage

import spock.lang.Specification

import java.time.LocalDateTime

class ClusterCacheEntryTest extends Specification {
    def "is cached if entry is valid and not expired"() {
        given:
        def entry = new ClusterCacheEntry("location", [], LocalDateTime.now().plusMinutes(1), true)

        expect:
        entry.isValidAndUnexpired()
    }

    def "is not cached if entry is not valid"() {
        given:
        def entry = new ClusterCacheEntry("location", [], LocalDateTime.now().plusMinutes(1), false)

        expect:
        !entry.isValidAndUnexpired()
    }

    def "is not cached if entry is expired"() {
        given:
        def entry = new ClusterCacheEntry("location", [], LocalDateTime.now().minusMinutes(1), true)

        expect:
        !entry.isValidAndUnexpired()
    }

    def "is not cached if entry is expired and not valid"() {
        given:
        def entry = new ClusterCacheEntry("location", [], LocalDateTime.now().minusMinutes(1), false)

        expect:
        !entry.isValidAndUnexpired()
    }
}
