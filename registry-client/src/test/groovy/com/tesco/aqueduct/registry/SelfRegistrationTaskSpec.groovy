package com.tesco.aqueduct.registry

import spock.lang.Specification

import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SelfRegistrationTaskSpec extends Specification {

    private static final String CLOUD_PIPE = "http://cloud.pipe"
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
    def registryHitList = Mock(RegistryHitList)

    def 'check registry client polls upstream service'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, registryHitList, { MY_NODE }, CLOUD_PIPE)

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.register(_ as Node) >> ["http://1.2.3.4", "http://5.6.7.8"]
        1 * registryHitList.update(_ as List) >> { startedLatch.countDown() }
    }

    def 'check registryHitList defaults to cloud pipe if register call fails'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, registryHitList, { MY_NODE }, CLOUD_PIPE)

        when: "register() is called"
        registryClient.register()

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        1 * upstreamClient.register(_ as Node) >> { throw new RuntimeException() }
        1 * registryHitList.update([new URL(CLOUD_PIPE)])
    }
}
