package com.tesco.aqueduct.registry

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import javax.sql.DataSource
import java.sql.DriverManager
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class PostgreSQLNodeRegistryIntegrationSpec extends Specification {

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    URL cloudURL = new URL("http://cloud.pipe:8080")
    DataSource dataSource
    NodeRegistry registry

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
            DROP TABLE IF EXISTS registry;
            
            CREATE TABLE registry(
            group_id VARCHAR PRIMARY KEY NOT NULL,
            entry JSON NOT NULL,
            version integer NOT NULL
            );
        """)

        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofDays(1))
    }

    def "registry always contains root"() {
        when: "I call summary on an empty registry"
        def summary = registry.getSummary(1234,"status", [])

        then: "Cloud root is returned"
        summary.root.localUrl == cloudURL
        summary.root.offset == 1234
        summary.root.status == "status"
    }

    def "registry accepts new elements"() {
        given: "A new node"

        ZonedDateTime now = ZonedDateTime.now()

        long offset = 12345

        Node expectedNode = Node.builder()
            .group("group")
            .localUrl(new URL("http://1.1.1.1"))
            .offset(offset)
            .status("following")
            .following([cloudURL])
            .providerLastAckOffset(offset-1)
            .providerLastAckTime(now)
            .lastSeen(now)
            .build()

        when: "The node is registered"
        registry.register(expectedNode)

        def followers = registry.getSummary(
            offset,
            "status",
            []
        ).followers

        then: "the registry contains the node"
        followers[0].group == "group"
        followers[0].offset == offset
        followers[0].status == "following"
        followers[0].following == [cloudURL]
        followers.size() == 1
    }

    @Unroll
    def "registry can filter by groups"() {
        given: "A registry with a few nodes in different groups"
        registerNode("groupA", "http://a1", 123, "following", [cloudURL])
        registerNode("groupB", "http://b1", 123, "following", [cloudURL])
        registerNode("groupC", "http://c1", 123, "following", [cloudURL])
        registerNode("groupA", "http://a2", 123, "following", [cloudURL])
        registerNode("groupB", "http://b2", 123, "following", [cloudURL])

        when: "Call summary with filters"
        def followers = registry.getSummary(1111, "status", filterGroups).followers

        then: "Groups returned have the expected node URL's in"
        followers*.localUrl*.toString().sort() == resultUrls

        where:
        filterGroups         | resultUrls
        ["groupA"]           | ["http://a1", "http://a2"]
        ["groupB"]           | ["http://b1", "http://b2"]
        ["groupC"]           | ["http://c1"]
        ["groupC"]           | ["http://c1"]
        ["groupA", "groupB"] | ["http://a1", "http://a2", "http://b1", "http://b2"]
        []                   | ["http://a1", "http://a2", "http://b1", "http://b2", "http://c1"]
    }

    def "the first till in the group to register gets assigned to the cloud"() {
        when: "Nodes are registered in different groups"
        def followA = registerNode("groupA", "http://a")
        def followB = registerNode("groupB", "http://a")
        def followC = registerNode("groupC", "http://a")

        then: "All first nodes in the group get a cloud URL"
        followA == [cloudURL]
        followB == [cloudURL]
        followC == [cloudURL]
    }

    def "the second till in the group should be told to calls the first till"() {
        given: "We have one till registered"
        registerNode("x", "http://first")

        when: "Second till registers"
        def follow = registerNode("x", "http://second")

        then: "It is told to call the first one"
        follow == [new URL("http://first"), cloudURL]
    }

    def "the third till in the group should be told to calls the first till and the cloud"() {
        given: "We have one till registered"
        registerNode("x", "http://first")

        and: "Second till registers"
        registerNode("x", "http://second")

        when: "Third till registers"
        def follow = registerNode("x", "http://third")

        then: "It is told to call the first one"
        follow == [new URL("http://first"), cloudURL]
    }


    def "registering the same till again returns the unchanged hierarchy"() {
        given: "We have two tills registered"
        def expectedFirst = registerNode("x", "http://first")
        def expectedSecond = registerNode("x", "http://second")

        when: "Both tills re-register"
        def actualFirst = registerNode("x", "http://first")
        def actualSecond = registerNode("x", "http://second")

        then: "It is told to call the cloud"
        actualFirst == expectedFirst
        actualSecond == expectedSecond
    }

    def "registering the same till again does not break the original hierarchy in storage"() {
        given: "some nodes"
        registerNode("x", "http://first")
        registerNode("x", "http://second")

        when: "it re-register itself"
        registerNode("x", "http://second")

        then: "It is told to call the cloud"
        registry.getSummary(0,"ok",[])
            .followers.find {it.localUrl.toString() == "http://second" }
            .requestedToFollow == [ new URL("http://first"), cloudURL ]
    }

    def "nodes can update state without changing hierachy"() {
        given: "two tills register"
        registerNode("x", "http://first")
        registerNode("x", "http://second")

        when: "first till re-registers"
        registerNode("x", "http://first", 0, "stale", [], null)

        then: "It's state is updated"
        List<Node> nodesState = registry.getSummary(0, "initialising", ["x"]).followers
        nodesState.toList().get(0).status == "stale"
    }

    @Unroll
    def "first follower defines a binary tree"() {
        given: "We have a store"
        registerNode("x", "http://1.1.1.0") // that's a cloud, we can ignore it
        def nodeToChildren = [:].withDefault {[]}

        when: "Nodes register"
        nodes.times {
            String address = "http://1.1.1.${it+1}"
            def firstFollower = registerNode("x", address).first()
            nodeToChildren[firstFollower] << address
        }
        nodeToChildren.each {
            println it
        }

        def oneChildren = 0
        def twoChildren = 0
        def otherCases = 0

        nodeToChildren.each { k,v ->
            if (k != cloudURL) {
                switch (v.size()) {
                    case 1:
                        oneChildren++
                        break
                    case 2:
                        twoChildren++
                        break
                    default:
                        otherCases++
                }
            }
        }

        then: "Expected number of one, two and more than one child nodes"
        oneChildren == expectedOneChildren
        otherCases == 0
        twoChildren == expectedTwoChilderen

        where:
        nodes | expectedOneChildren | expectedTwoChilderen | comment
        32    | 0                   | 16                   | "full tree has two children or no children"
        31    | 1                   | 15                   | "binary tree missing one node to be full"
        30    | 0                   | 15                   | "binary tree missing two node to be full, we add nodes left to right"
        //30 | 3                |  "binary tree missing two node to be full, we should spread load evenly" // not for now
    }

    def "explicit test of the binary tree logic"() {
        when: "nodes register"
        def nodeList1 = registerNode("x", "http://1.1.1.1").toListString()
        def nodeList2 = registerNode("x", "http://1.1.1.2").toListString()
        def nodeList3 = registerNode("x", "http://1.1.1.3").toListString()
        def nodeList4 = registerNode("x", "http://1.1.1.4").toListString()
        def nodeList5 = registerNode("x", "http://1.1.1.5").toListString()
        def nodeList6 = registerNode("x", "http://1.1.1.6").toListString()
        def nodeList7 = registerNode("x", "http://1.1.1.7").toListString()
        def nodeList8 = registerNode("x", "http://1.1.1.8").toListString()

        then: "The heirachy returned to each node forms a binary tree"
        nodeList1 == "[http://cloud.pipe:8080]"
        nodeList2 == "[http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList3 == "[http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList4 == "[http://1.1.1.2, http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList5 == "[http://1.1.1.2, http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList6 == "[http://1.1.1.3, http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList7 == "[http://1.1.1.3, http://1.1.1.1, http://cloud.pipe:8080]"
        nodeList8 == "[http://1.1.1.4, http://1.1.1.2, http://1.1.1.1, http://cloud.pipe:8080]"
    }

    @Unroll
    def "node registry can handle group level concurrent requests safely #name"() {
        when: "nodes register concurrently"
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        PollingConditions conditions = new PollingConditions(timeout: timeout)
        for(int i =0 ; i<iterations; i++) {
            int j = i
            pool.execute{
                registerNode("x", "http://" + j, j)
            }
        }

        then: "summary of the registry is as expected"
        conditions.eventually {
            assert registry.getSummary(0, "initialising", ["x"]).followers.size() == iterations
        }

        cleanup:
        pool.shutdown()

        where:

        iterations | threads | timeout | name
        2          | 2       | 5       | "case A" // we have seen bad changes to the code that make case A and B pass/fail in a non-deterministic way, hence they're here
        2          | 2       | 5       | "case B"
        1          | 1       | 1       | "1 till"
        10         | 1       | 1       | "1 till 10 calls"
        2          | 2       | 1       | "2 tills"
        10         | 2       | 1       | "2 tills 10 calls"
        3          | 3       | 1       | "3 tills"
        10         | 3       | 1       | "3 tills 10 calls"
        100        | 3       | 2       | "3 tills 100 calls"
        500        | 3       | 14      | "3 tills 500 calls"
        10         | 10      | 1       | "10 tills"
        50         | 50      | 1       | "50 tills"
        100        | 100     | 2       | "100 tills"
        250        | 250     | 5       | "250 tills"
    }

    @Unroll
    def "node registry can handle concurrent multiple group requests (10 stores with 50 tills each)"() {
        when: "nodes register concurrently"
        ExecutorService pool = Executors.newFixedThreadPool(500)
        PollingConditions conditions = new PollingConditions(timeout: 10)
        for(int i =0 ; i<500; i++) {
            int j = i
            pool.execute{
                registerNode("x" + j % 10, "http://" + j, j)
            }
        }

        then: "summary of the registry is as expected"
        conditions.eventually {
            assert registry.getSummary(0, "initialising", ["x0"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x1"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x2"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x3"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x4"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x5"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x6"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x7"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x8"]).followers.size() == 50
            assert registry.getSummary(0, "initialising", ["x9"]).followers.size() == 50
        }

        cleanup:
        pool.shutdown()
    }

    def "After some time nodes are marked as offline"() {
        given: "registry with small offline mark"
        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofMillis(100))

        def offset = 100
        def now = ZonedDateTime.now()

        Node theNode = Node.builder()
            .group("x")
            .localUrl(new URL("http://1.1.1.1"))
            .offset(offset)
            .status("initialising")
            .following([])
            .lastSeen(now)
            .providerLastAckTime(now)
            .providerLastAckOffset(1)
            .build()

        registry.register(theNode)

        when: "an following node"
        def node = registry.getSummary(0, "initialising", []).followers.first()
        Thread.sleep(300)
        def nodeAfterSomeTime = registry.getSummary(0, "initialising", []).followers.first()

        then:
        node.status == "initialising"
        nodeAfterSomeTime.status == "offline"
    }

    // provided hierarchy, vs expected hierarchy
    // update last seen date

    def registerNode(
        String group,
        String url,
        long offset=0,
        String status="initialising",
        List<URL> following=[],
        List<URL> requestedToFollow=[],
        ZonedDateTime created=null
    ) {
        def now = ZonedDateTime.now()

        Node theNode = Node.builder()
            .localUrl(new URL(url))
            .group(group)
            .status(status)
            .offset(offset)
            .following(following)
            .lastSeen(created)
            .requestedToFollow(requestedToFollow)
            .providerLastAckOffset(offset-1)
            .providerLastAckTime(now)
            .build()

        registry.register(theNode)
    }
}
