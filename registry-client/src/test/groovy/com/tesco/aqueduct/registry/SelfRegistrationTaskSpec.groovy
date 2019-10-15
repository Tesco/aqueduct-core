package com.tesco.aqueduct.registry

import com.tesco.aqueduct.registry.model.Node
import spock.lang.Specification

import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SelfRegistrationTaskSpec extends Specification {
    private static final URL MY_HOST = new URL("http://localhost")

    private static final Node MY_NODE = Node.builder()
        .group("1234")
        .localUrl(MY_HOST)
        .offset(0)
        .status("initialising")
        .following(Collections.emptyList())
        .lastSeen(ZonedDateTime.now())
        .build()

    def upstreamClient = Mock(RegistryClient)
    def services = Mock(ServiceList)

    def 'check registry client polls upstream service'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services)

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.register(_ as Node) >> ["http://1.2.3.4", "http://5.6.7.8"]
        1 * services.update(_ as List) >> { startedLatch.countDown() }
    }

    def 'check registryHitList defaults to cloud pipe if register call fails'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services)

        and: "when called, upstreamClient will throw an exception"
        upstreamClient.register(_ as Node) >> { throw new RuntimeException() }

        when: "register() is called"
        registryClient.register()

        then: "services are NOT updated"
        0 * services.update(_)
    }

    def 'check register doesnt default to cloud pipe if previously it succeeded'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services)
        upstreamClient.register(_ as Node) >> ["http://1.2.3.4", "http://5.6.7.8"] >> { throw new RuntimeException() }

        when: "register() is called successfully"
        registryClient.register()

        and: "register is called again, and this time is unsuccessful"
        registryClient.register()

        then: "hitlist has only been called once, upstream client is registered to twice"
        1 * services.update(["http://1.2.3.4", "http://5.6.7.8"])
    }

    def 'null response to register call doesnt result in null hit list update update'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services)

        and: "upstream client will return null"
        upstreamClient.register(_ as Node) >> { null }

        when: "register() is called"
        registryClient.register()

        then: "Services are NOT updated"
        0 * services.update(_)
    }
}
