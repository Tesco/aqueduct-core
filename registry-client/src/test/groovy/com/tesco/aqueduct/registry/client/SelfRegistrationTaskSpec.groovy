package com.tesco.aqueduct.registry.client

import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Bootstrapable
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.RegistryResponse
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class SelfRegistrationTaskSpec extends Specification {
    private static final URL MY_HOST = new URL("http://localhost")

    private static final Node MY_NODE = Node.builder()
        .group("1234")
        .localUrl(MY_HOST)
        .offset(0)
        .status(INITIALISING)
        .following(Collections.emptyList())
        .lastSeen(ZonedDateTime.now())
        .build()

    def upstreamClient = Mock(RegistryClient)
    def services = Mock(ServiceList)
    def bootstrapableProvider = Mock(Bootstrapable)
    def bootstrapablePipe = Mock(Bootstrapable)

    def 'check registry client polls upstream service'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe)

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.register(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], BootstrapType.NONE)
        1 * services.update(_ as List) >> { startedLatch.countDown() }
    }

    def 'check registryHitList defaults to cloud pipe if register call fails'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe)

        and: "when called, upstreamClient will throw an exception"
        upstreamClient.register(_ as Node) >> { throw new RuntimeException() }

        when: "register() is called"
        registryClient.register()

        then: "services are NOT updated"
        0 * services.update(_)
    }

    def 'check register doesnt default to cloud pipe if previously it succeeded'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe)
        upstreamClient.register(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], BootstrapType.NONE) >> { throw new RuntimeException() }

        when: "register() is called successfully"
        registryClient.register()

        and: "register is called again, and this time is unsuccessful"
        registryClient.register()

        then: "hitlist has only been called once, upstream client is registered to twice"
        1 * services.update(["http://1.2.3.4", "http://5.6.7.8"])
    }

    def 'null response to register call doesnt result in null hit list update update'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe)

        and: "upstream client will return null"
        upstreamClient.register(_ as Node) >> { null }

        when: "register() is called"
        registryClient.register()

        then: "Services are NOT updated"
        0 * services.update(_)
    }

    @Unroll
    def 'bootstrap related methods are called in correct combo depending on bootstrap type'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe)

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.register(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], bootstrapType)
        1 * services.update(_ as List) >> { startedLatch.countDown() }
        numProviderBootstrapCalls * bootstrapableProvider.reset()
        numProviderBootstrapCalls * bootstrapableProvider.start()
        numPipeBootstrapCalls * bootstrapablePipe.reset()
        numPipeBootstrapCalls * bootstrapablePipe.start()

        where:
        bootstrapType                   | numProviderBootstrapCalls | numPipeBootstrapCalls
        BootstrapType.PROVIDER          | 1                         | 0
        BootstrapType.PIPE_AND_PROVIDER | 1                         | 1
        BootstrapType.NONE              | 0                         | 0
    }
}
