package com.tesco.aqueduct.registry.postgres

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.NodeRegistry
import com.tesco.aqueduct.registry.model.Status
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import javax.sql.DataSource
import java.sql.DriverManager
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.tesco.aqueduct.registry.model.Status.FOLLOWING
import static com.tesco.aqueduct.registry.model.Status.INITIALISING
import static com.tesco.aqueduct.registry.model.Status.OFFLINE
import static com.tesco.aqueduct.registry.model.Status.OK
import static com.tesco.aqueduct.registry.model.Status.PENDING

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
        def summary = registry.getSummary(1234, FOLLOWING, [])

        then: "Cloud root is returned"
        summary.root.localUrl == cloudURL
        summary.root.offset == 1234
        summary.root.status == FOLLOWING
    }

    def "registry accepts new elements"() {
        given: "A new node"

        ZonedDateTime now = ZonedDateTime.now()

        long offset = 12345

        Node expectedNode = createNode("group", new URL("http://1.1.1.1"), offset, FOLLOWING, [cloudURL], now)

        when: "The node is registered"
        registry.register(expectedNode)

        def followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then: "the registry contains the node"
        followers[0].group == "group"
        followers[0].offset == offset
        followers[0].status == FOLLOWING
        followers[0].following == [cloudURL]
        followers.size() == 1
    }

    def "registry marks nodes offline and sorts based on status"() {
        given: "a registry with a short offline delta"
        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofSeconds(5))

        and: "6 nodes"
        long offset = 12345

        URL url1 = new URL("http://1.1.1.1")
        Node node1 = createNode("group", url1, offset, FOLLOWING, [cloudURL])

        URL url2 = new URL("http://2.2.2.2")
        Node node2 = createNode("group", url2, offset, FOLLOWING, [cloudURL])

        URL url3 = new URL("http://3.3.3.3")
        Node node3 = createNode("group", url3, offset, FOLLOWING, [cloudURL])

        URL url4= new URL("http://4.4.4.4")
        Node node4 = createNode("group", url4, offset, FOLLOWING, [cloudURL])

        URL url5 = new URL("http://5.5.5.5")
        Node node5 = createNode("group", url5, offset, FOLLOWING, [cloudURL])

        URL url6 = new URL("http://6.6.6.6")
        Node node6 = createNode("group", url6, offset, FOLLOWING, [cloudURL])

        when: "nodes are registered"
        registry.register(node1)
        registry.register(node2)
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)
        registry.register(node6)

        and: "half fail to re-register within the offline delta"
        sleep 5000
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)


        and: "get summary"
        def followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then: "nodes are marked as offline and sorted accordingly"
        followers[0].getLocalUrl() == url3
        followers[1].getLocalUrl() == url4
        followers[2].getLocalUrl() == url5
        followers[3].getLocalUrl() == url1
        followers[4].getLocalUrl() == url2
        followers[5].getLocalUrl() == url6

        followers[0].status == FOLLOWING
        followers[1].status == FOLLOWING
        followers[2].status == FOLLOWING
        followers[3].status == OFFLINE
        followers[4].status == OFFLINE
        followers[5].status == OFFLINE

        followers[0].requestedToFollow == [cloudURL]
        followers[1].requestedToFollow == [url3, cloudURL]
        followers[2].requestedToFollow == [url3, cloudURL]
        followers[3].requestedToFollow == [url4, url3, cloudURL]
        followers[4].requestedToFollow == [url4, url3, cloudURL]
        followers[5].requestedToFollow == [url5, url3, cloudURL]
    }

    @Unroll
    def "registry can filter by groups"() {
        given: "A registry with a few nodes in different groups"
        registerNode("groupA", "http://a1", 123, FOLLOWING, [cloudURL])
        registerNode("groupB", "http://b1", 123, FOLLOWING, [cloudURL])
        registerNode("groupC", "http://c1", 123, FOLLOWING, [cloudURL])
        registerNode("groupA", "http://a2", 123, FOLLOWING, [cloudURL])
        registerNode("groupB", "http://b2", 123, FOLLOWING, [cloudURL])

        when: "Call summary with filters"
        def followers = registry.getSummary(1111, FOLLOWING, filterGroups).followers

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

    def "the first node in the group to register gets assigned to the cloud"() {
        when: "Nodes are registered in different groups"
        def followA = registerNode("groupA", "http://a")
        def followB = registerNode("groupB", "http://a")
        def followC = registerNode("groupC", "http://a")

        then: "All first nodes in the group get a cloud URL"
        followA == [cloudURL]
        followB == [cloudURL]
        followC == [cloudURL]
    }

    def "the second node in the group should be told to calls the first node"() {
        given: "We have one node registered"
        registerNode("x", "http://first")

        when: "Second node registers"
        def follow = registerNode("x", "http://second")

        then: "It is told to call the first one"
        follow == [new URL("http://first"), cloudURL]
    }

    def "the third node in the group should be told to call the first node and the cloud"() {
        given: "We have one node registered"
        registerNode("x", "http://first")

        and: "Second node registers"
        registerNode("x", "http://second")

        when: "Third node registers"
        def follow = registerNode("x", "http://third")

        then: "It is told to call the first one"
        follow == [new URL("http://first"), cloudURL]
    }


    def "registering the same node again returns the unchanged hierarchy"() {
        given: "We have two nodes registered"
        def expectedFirst = registerNode("x", "http://first")
        def expectedSecond = registerNode("x", "http://second")

        when: "Both nodes re-register"
        def actualFirst = registerNode("x", "http://first")
        def actualSecond = registerNode("x", "http://second")

        then: "It is told to call the cloud"
        actualFirst == expectedFirst
        actualSecond == expectedSecond
    }

    def "registering the same node again does not break the original hierarchy in storage"() {
        given: "some nodes"
        registerNode("x", "http://first")
        registerNode("x", "http://second")

        when: "it re-register itself"
        registerNode("x", "http://second")

        then: "It is told to call the cloud"
        registry.getSummary(0,OK,[])
            .followers.find {it.localUrl.toString() == "http://second" }
            .requestedToFollow == [ new URL("http://first"), cloudURL ]
    }

    def "nodes can update state without changing hierarchy"() {
        given: "two nodes register"
        registerNode("x", "http://first")
        registerNode("x", "http://second")

        when: "first node re-registers"
        registerNode("x", "http://first", 0, PENDING, [])

        then: "It's state is updated"
        List<Node> nodesState = registry.getSummary(0, INITIALISING, ["x"]).followers
        nodesState.toList().get(0).status == PENDING
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
        PollingConditions conditions = new PollingConditions(timeout: timeout)

        nodes.times{ i ->
            new Thread( {
                registerNode("x", "http://" + i, i)
            }).run()
        }

        then: "summary of the registry is as expected"
        conditions.eventually {
            assert registry.getSummary(0, INITIALISING, ["x"]).followers.size() == nodes
        }

        where:
        nodes | threads | timeout | name
        1     | 1       | 1       | "1 node with concurrency 1"
        10    | 1       | 1       | "10 node with concurrency 1"
        2     | 2       | 1       | "2 nodes with concurrency 1"
        10    | 2       | 5       | "10 nodes with concurrency 2"
        3     | 3       | 1       | "3 nodes with concurrency 3"
        10    | 3       | 5       | "10 nodes with concurrency 3"
        100   | 3       | 5       | "100 nodes with concurrency 3"
        300   | 3       | 20      | "300 nodes with concurrency 3"
        10    | 10      | 3       | "10 nodes with concurrency 10"
        50    | 50      | 3       | "50 nodes with concurrency 50"
        100   | 100     | 5       | "100 nodes with concurrency 100"
        250   | 250     | 10      | "250 nodes with concurrency 250"
    }

    def "node registry can handle concurrent multiple group requests (10 stores with 50 nodes each)"() {
        when: "nodes register concurrently"
        def nodes = 500
        ExecutorService pool = Executors.newFixedThreadPool(nodes)
        PollingConditions conditions = new PollingConditions(timeout: 10)
        nodes.times{ i ->
            pool.execute{
                registerNode("x" + i % 10, "http://" + i, i)
            }
        }

        then: "summary of the registry is as expected"
        conditions.eventually {
            assert registry.getSummary(0, INITIALISING, ["x0"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x1"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x2"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x3"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x4"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x5"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x6"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x7"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x8"]).followers.size() == 50
            assert registry.getSummary(0, INITIALISING, ["x9"]).followers.size() == 50
        }

        cleanup:
        pool.shutdown()
    }

    def "groups are ordered in get summary"() {
        given: "5 groups registered"
        registerNode("x2", "http://2")
        registerNode("x5", "http://5")
        registerNode("x3", "http://3")
        registerNode("x4", "http://4")
        registerNode("x1", "http://1")

        when: "summary is taken"
        def summary = registry.getSummary(0, INITIALISING, [])

        then: "groups are returned in lexicographical order"
        summary.followers*.group == ["x1", "x2", "x3", "x4", "x5"]
    }

    def "when there is contention for first node, this is handled safely"() {
        given:
        int nodes = 200
        int threads = nodes

        when: "100 nodes register concurrently"
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        PollingConditions conditions = new PollingConditions(timeout: 10)

        CompletableFuture startLock = new CompletableFuture()

        nodes.times { i ->
            pool.execute{
                startLock.get()
                registerNode("x", "http://" + i, i)
            }
        }

        startLock.complete(true)

        then: "summary of the registry is as expected"
        conditions.eventually {
            assert registry.getSummary(0, INITIALISING, ["x"]).followers.size() == nodes
        }

        cleanup:
        pool.shutdown()
    }

    def "After some time nodes are marked as offline"() {
        given: "registry with small offline mark"
        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofMillis(100))

        def offset = 100
        def now = ZonedDateTime.now()

        Node theNode = createNode("x", new URL("http://1.1.1.1"), offset, INITIALISING, [], now)

        registry.register(theNode)

        when: "an following node"
        def node = registry.getSummary(0, INITIALISING, []).followers.first()
        Thread.sleep(300)
        def nodeAfterSomeTime = registry.getSummary(0, INITIALISING, []).followers.first()

        then:
        node.status == INITIALISING
        nodeAfterSomeTime.status == OFFLINE
    }

    def "Delete node from database if exists"() {
        given: "two nodes register"
        registerNode("x", "http://first")
        registerNode("x", "http://second")

        when: "delete a node"
        registry.deleteNode("x", "first")

        then: "It's state is updated"
        List<Node> nodesState = registry.getSummary(0, INITIALISING, ["x"]).followers
        nodesState.size() == 1
        nodesState.get(0).id == "x|http://second"
    }

    @Ignore
    def "the second node in the group with different version to first node should get its own hierarchy"() {
        given: "We have one node registered"
        def firstNode = registerNode("groupA", "http://a1", 123, FOLLOWING, [], ["v": "1.0"])

        when: "Second node registers"
        def secondNode = registerNode("groupA", "http://a2", 123, FOLLOWING, [], ["v":"1.1"])

        then: "both nodes follow cloud"
        firstNode == [cloudURL]
        secondNode == [cloudURL]
    }

    @Ignore
    def "having third node in the group with different version to first and second node should make three hierarchies"() {
        given: "We have two nodes registered with different version"
        def firstNode = registerNode("groupA", "http://a1", 123, FOLLOWING, [], ["v": "1.0"])
        def secondNode = registerNode("groupA", "http://a2", 123, FOLLOWING, [], ["v":"1.1"])

        when: "third node registers with a new version"
        def thirdNode = registerNode("groupA", "http://a3", 123, FOLLOWING, [], ["v":"2.0"])

        then: "all three nodes follow cloud"
        firstNode == [cloudURL]
        secondNode == [cloudURL]
        thirdNode == [cloudURL]
    }

    def "having third node switched to older version make it join old version hierarchy"() {
        given: "We have two nodes registered with different version"
        def firstNode = registerNode("groupA", "http://a1", 123, FOLLOWING, [], ["v": "1.0"])
        def secondNode = registerNode("groupA", "http://a2", 123, FOLLOWING, [], ["v":"1.1"])

        when: "second node registers with an old version"
        secondNode = registerNode("groupA", "http://a2", 123, FOLLOWING, [], ["v":"1.0"])

        then: "all three nodes follow cloud"
        firstNode == [cloudURL]
        secondNode == [new URL("http://a1"), cloudURL]
    }

    @Ignore
    def "on new version appearing for an existing node, the hierarchy splits into two trees"() {
        given: "2 nodes"
        long offset = 12345

        URL url1 = new URL("http://1.1.1.1")
        Node node1 = createNode("group", url1, offset, FOLLOWING, [cloudURL])

        URL url2 = new URL("http://2.2.2.2")
        Node node2 = createNode("group", url2, offset, FOLLOWING, [cloudURL])

        when: "nodes are registered"
        registry.register(node1)
        registry.register(node2)

        and: "get summary"
        def followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then:
        followers[0].requestedToFollow == [cloudURL]
        followers[1].requestedToFollow == [url1, cloudURL]

        when: "a new version is deployed to a node"
        node2 = createNode("group", url2, offset, FOLLOWING, [cloudURL], null, ["v":"2.0"])
        registry.register(node2)

        and: "get summary"
        followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then: "the hierarchy splits by version"
        followers[0].requestedToFollow == [cloudURL]
        followers[1].requestedToFollow == [cloudURL]
    }

    @Ignore
    def "registry marks nodes offline and sorts based on status within their hierarchies"() {
        given: "a registry with a short offline delta"
        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofSeconds(5))

        and: "6 nodes with different versions"
        long offset = 12345

        URL url1 = new URL("http://1.1.1.1")
        Node node1 = createNode("group", url1, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url2 = new URL("http://2.2.2.2")
        Node node2 = createNode("group", url2, offset, FOLLOWING, [cloudURL], null, ["v":"1.1"])

        URL url3 = new URL("http://3.3.3.3")
        Node node3 = createNode("group", url3, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url4 = new URL("http://4.4.4.4")
        Node node4 = createNode("group", url4, offset, FOLLOWING, [cloudURL], null, ["v":"1.1"])

        URL url5 = new URL("http://5.5.5.5")
        Node node5 = createNode("group", url5, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url6 = new URL("http://6.6.6.6")
        Node node6 = createNode("group", url6, offset, FOLLOWING, [cloudURL], null, ["v":"2.0"])

        when: "nodes are registered"
        registry.register(node1)
        registry.register(node2)
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)
        registry.register(node6)

        and: "half fail to re-register within the offline delta"
        sleep 5000
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)

        and: "get summary"
        def followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then: "nodes are marked as offline and sorted accordingly"
        // version 1.0
        followers[0].getLocalUrl() == url3
        followers[1].getLocalUrl() == url5
        followers[2].getLocalUrl() == url1

        // version 1.1
        followers[3].getLocalUrl() == url4
        followers[4].getLocalUrl() == url2

        // version 2.0
        followers[5].getLocalUrl() == url6

        // version 1.0
        followers[0].status == FOLLOWING
        followers[1].status == FOLLOWING
        followers[2].status == OFFLINE

        // version 1.1
        followers[3].status == FOLLOWING
        followers[4].status == OFFLINE

        // version 2.0
        followers[5].status == OFFLINE

        // version 1.0
        followers[0].requestedToFollow == [cloudURL]
        followers[1].requestedToFollow == [url3, cloudURL]
        followers[2].requestedToFollow == [url3, cloudURL]

        // version 1.1
        followers[3].requestedToFollow == [cloudURL]
        followers[4].requestedToFollow == [url4, cloudURL]

        // version 2.0
        followers[5].requestedToFollow == [cloudURL]
    }

    def "registry marks nodes offline and sorts nodes ignoring version"() {
        given: "a registry with a short offline delta"
        registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofSeconds(5))

        and: "6 nodes with different versions"
        long offset = 12345

        URL url1 = new URL("http://1.1.1.1")
        Node node1 = createNode("group", url1, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url2 = new URL("http://2.2.2.2")
        Node node2 = createNode("group", url2, offset, FOLLOWING, [cloudURL], null, ["v":"1.1"])

        URL url3 = new URL("http://3.3.3.3")
        Node node3 = createNode("group", url3, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url4 = new URL("http://4.4.4.4")
        Node node4 = createNode("group", url4, offset, FOLLOWING, [cloudURL], null, ["v":"1.1"])

        URL url5 = new URL("http://5.5.5.5")
        Node node5 = createNode("group", url5, offset, FOLLOWING, [cloudURL], null, ["v":"1.0"])

        URL url6 = new URL("http://6.6.6.6")
        Node node6 = createNode("group", url6, offset, FOLLOWING, [cloudURL], null, ["v":"2.0"])

        when: "nodes are registered"
        registry.register(node1)
        registry.register(node2)
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)
        registry.register(node6)

        and: "half fail to re-register within the offline delta"
        sleep 5000
        registry.register(node3)
        registry.register(node4)
        registry.register(node5)

        and: "get summary"
        def followers = registry.getSummary(
            offset,
            FOLLOWING,
            []
        ).followers

        then: "nodes are marked as offline and sorted accordingly"
        // version 1.0
        followers[0].getLocalUrl() == url3
        followers[1].getLocalUrl() == url4
        followers[2].getLocalUrl() == url5

        // version 1.1
        followers[3].getLocalUrl() == url1
        followers[4].getLocalUrl() == url2

        // version 2.0
        followers[5].getLocalUrl() == url6

        // version 1.0
        followers[0].status == FOLLOWING
        followers[1].status == FOLLOWING
        followers[2].status == FOLLOWING

        // version 1.1
        followers[3].status == OFFLINE
        followers[4].status == OFFLINE

        // version 2.0
        followers[5].status == OFFLINE

        // version 1.0
        followers[0].requestedToFollow == [cloudURL]
        followers[1].requestedToFollow == [url3, cloudURL]
        followers[2].requestedToFollow == [url3, cloudURL]

        // version 1.1
        followers[3].requestedToFollow == [url4, url3, cloudURL]
        followers[4].requestedToFollow == [url4, url3, cloudURL]

        // version 2.0
        followers[5].requestedToFollow == [url5, url3, cloudURL]
    }

    // provided hierarchy, vs expected hierarchy
    // update last seen date

    def registerNode(
        String group,
        String url,
        long offset=0,
        Status status=INITIALISING,
        List<URL> following=[],
        Map<String, String> pipeProperties=["v":"1.0"],
        List<URL> requestedToFollow=[],
        ZonedDateTime created=null
    ) {
        Node theNode = Node.builder()
            .localUrl(new URL(url))
            .group(group)
            .status(status)
            .offset(offset)
            .following(following)
            .lastSeen(created)
            .requestedToFollow(requestedToFollow)
            .pipe(pipeProperties)
            .build()

        registry.register(theNode)
    }

    def createNode(
        String group,
        URL url,
        long offset=0,
        Status status=INITIALISING,
        List<URL> following=[],
        ZonedDateTime created=null,
        Map<String, String> pipeProperties=["v":"1.0"],
        List<URL> requestedToFollow=[]
    ) {
        return Node.builder()
            .localUrl(url)
            .group(group)
            .status(status)
            .offset(offset)
            .following(following)
            .lastSeen(created)
            .requestedToFollow(requestedToFollow)
            .pipe(pipeProperties)
            .build()
    }
}
