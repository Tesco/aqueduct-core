package com.tesco.aqueduct.registry.client

import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Bootstrapable
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.RegistryResponse
import com.tesco.aqueduct.registry.model.Resetable
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class SelfRegistrationTaskSpec extends Specification {
    private static final URL MY_HOST = new URL("http://localhost")
    private static final String REGISTRATION_INTERVAL = "1s";
    private static final int BOOTSTRAP_DELAY = 500;

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
    def corruptionManager = Mock(Resetable)

    def 'check registry client polls upstream service'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe, corruptionManager, REGISTRATION_INTERVAL, BOOTSTRAP_DELAY)

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
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe, corruptionManager, REGISTRATION_INTERVAL, BOOTSTRAP_DELAY)

        and: "when called, upstreamClient will throw an exception"
        upstreamClient.register(_ as Node) >> { throw new RuntimeException() }

        when: "register() is called"
        registryClient.register()

        then: "services are NOT updated"
        0 * services.update(_)
    }

    def 'check register does not default to cloud pipe if previously it succeeded'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe, corruptionManager, REGISTRATION_INTERVAL, BOOTSTRAP_DELAY)
        upstreamClient.register(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], BootstrapType.NONE) >> { throw new RuntimeException() }

        when: "register() is called successfully"
        registryClient.register()

        and: "register is called again, and this time is unsuccessful"
        registryClient.register()

        then: "hitlist has only been called once, upstream client is registered to twice"
        1 * services.update(["http://1.2.3.4", "http://5.6.7.8"])
    }

    def 'null response to register call does not result in null hit list update update'() {
        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe, corruptionManager, REGISTRATION_INTERVAL, BOOTSTRAP_DELAY)

        and: "upstream client will return null"
        upstreamClient.register(_ as Node) >> { null }

        when: "register() is called"
        registryClient.register()

        then: "Services are NOT updated"
        0 * services.update(_)
    }

    @Unroll
    def 'bootstrap related methods are called in correct combo and order depending on bootstrap type'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = new SelfRegistrationTask(upstreamClient, { MY_NODE }, services, bootstrapableProvider, bootstrapablePipe, corruptionManager, REGISTRATION_INTERVAL, BOOTSTRAP_DELAY)

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.register(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], bootstrapType)
        1 * services.update(_ as List) >> { startedLatch.countDown() }

        then: "provider is stopped"
        providerStopAndResetCalls * bootstrapableProvider.stop()

        then: "provider is reset"
        providerStopAndResetCalls * bootstrapableProvider.reset()

        then: "pipe is stopped"
        pipeStopCalls * bootstrapablePipe.stop()

        then: "corruption manager is reset"
        corruptionManagerCalls * corruptionManager.reset()

        then: "pipe is reset"
        pipeResetAndStartCalls * bootstrapablePipe.reset()

        then: "pipe is started"
        pipeResetAndStartCalls * bootstrapablePipe.start()

        then: "provider is started"
        providerStartCalls * bootstrapableProvider.start()

        where:
        bootstrapType                              | providerStopAndResetCalls | providerStartCalls | pipeResetAndStartCalls | pipeStopCalls | corruptionManagerCalls
        BootstrapType.PROVIDER                     | 1                         | 1                  | 0                      | 0             | 0
        BootstrapType.PIPE_AND_PROVIDER            | 1                         | 1                  | 1                      | 1             | 0
        BootstrapType.NONE                         | 0                         | 0                  | 0                      | 0             | 0
        BootstrapType.PIPE                         | 0                         | 0                  | 1                      | 1             | 0
        BootstrapType.PIPE_WITH_DELAY              | 0                         | 0                  | 1                      | 1             | 0
        BootstrapType.PIPE_AND_PROVIDER_WITH_DELAY | 1                         | 1                  | 1                      | 1             | 0
        BootstrapType.CORRUPTION_RECOVERY          | 1                         | 0                  | 0                      | 1             | 1
    }
}
