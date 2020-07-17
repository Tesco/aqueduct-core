package com.tesco.aqueduct.registry.utils

import com.tesco.aqueduct.registry.model.Node
import org.slf4j.Logger
import org.slf4j.MDC
import spock.lang.Specification

import java.time.ZonedDateTime

import static com.tesco.aqueduct.pipe.api.PipeState.UP_TO_DATE
import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class RegistryLoggerSpec extends Specification {

    def "Logger logs as expected with message"() {
        given:
        Logger logger = Mock()
        logger.isDebugEnabled() >> true
        RegistryLogger LOG = new RegistryLogger(logger)

        when:
        LOG.debug("testWhere", "testWhat")

        then:
        1 * logger.debug("testWhat")
    }

    def "Logger doesn't log debug messages when debug is disabled"() {
        given:
        Logger logger = Mock()
        logger.isDebugEnabled() >> false
        RegistryLogger LOG = new RegistryLogger(logger)

        when:
        LOG.debug("testWhere", "testWhat")

        then:
        0 * logger.debug("testWhat")
    }

    def "Logging errors"() {
        given:
        Logger logger = Mock()

        RegistryLogger LOG = new RegistryLogger(logger)

        when:
        LOG.error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }

    def "Logging fields without a message"() {
        given:
        Logger logger = Mock()

        RegistryLogger LOG = new RegistryLogger(logger)

        when:
        LOG.error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }

    def "Logging with a node at error level"() {
        given:
        Logger logger = Mock()

        URL url = new URL("http://localhost")

        RegistryLogger LOG = new RegistryLogger(logger)
        def myNode = Node.builder()
            .group("1234")
            .localUrl(url)
            .following([url])
            .requestedToFollow([url])
            .offset(0)
            .pipe(["pipeState": UP_TO_DATE] as Map<String, String>)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        when:
        LOG.withNode(myNode).error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }

    def "Logging with a node at info level"() {
        given:
        Logger logger = Mock()
        logger.isInfoEnabled() >> true

        URL url = new URL("http://localhost")

        RegistryLogger LOG = new RegistryLogger(logger)
        def myNode = Node.builder()
            .group("1234")
            .localUrl(url)
            .following([url])
            .requestedToFollow([url])
            .offset(0)
            .pipe(["pipeState": UP_TO_DATE] as Map<String, String>)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        when:
        LOG.withNode(myNode).info("testWhere", "testWhat")

        then:
        true
        1 * logger.info("testWhat")
    }

    def "Logging with a node at info level and some null fields"() {
        given:
        Logger logger = Mock()
        logger.isInfoEnabled() >> true

        URL url = new URL("http://localhost")

        RegistryLogger LOG = new RegistryLogger(logger)
        def myNode = Node.builder()
            .group("1234")
            .localUrl(url)
            .offset(0)
            .pipe(["pipeState": UP_TO_DATE] as Map<String, String>)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        when:
        LOG.withNode(myNode).info("testWhere", "testWhat")

        then:
        true
        1 * logger.info("testWhat")
    }

    def "Logging preserves MDC state as before log invocation and clears MDC state generated for this log"() {
        given:
        Logger logger = Mock()
        logger.isInfoEnabled() >> true

        URL url = new URL("http://localhost")

        RegistryLogger LOG = new RegistryLogger(logger)
        def myNode = Node.builder()
            .group("1234")
            .localUrl(url)
            .offset(0)
            .pipe(["pipeState": UP_TO_DATE] as Map<String, String>)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()

        and: "MDC has some state"
        MDC.put("trace_id", "someTraceId")

        when:
        LOG.withNode(myNode).info("testWhere", "testWhat")

        then: "MDC state is preserved as before"
        MDC.getCopyOfContextMap() == ["trace_id":"someTraceId"]

        and: "log info is invoked"
        1 * logger.info("testWhat")
    }
}