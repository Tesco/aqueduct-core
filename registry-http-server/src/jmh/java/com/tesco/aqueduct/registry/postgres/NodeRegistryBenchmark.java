package com.tesco.aqueduct.registry.postgres;

import com.beust.jcommander.internal.Lists;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeGroup;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.registry.model.Status;
import groovy.sql.Sql;
import org.apache.groovy.util.Maps;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Fork(value = 0, warmups = 1)
@Warmup(iterations = 3, time=5)
@Measurement(iterations = 10, time = 5)
public class NodeRegistryBenchmark {

    private static List<String> versions = Lists.newArrayList("1.0","1.1","2.0");
    private static Map<String, List<Node>> nodesMap = new HashMap<>(400);
    private static List<Node> allNodes;

    private static URL cloudURL;
    private static DataSource dataSource;

    private static String randomVersion() {
        return versions.get(ThreadLocalRandom.current().nextInt(3));
    }

    @State(Scope.Benchmark)
    public static class PostgresRegistry {

        private NodeRegistry registry;
        private Sql sql;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {

            System.out.println("Setting up database...");
            cloudURL = new URL("http://cloud.pipe:8080");
            setupDatabase();

            allNodes = nodesMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
            registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofDays(1));
        }


        @TearDown(Level.Trial)
        public void doTearDown() {
            sql.close();
        }

        private void setupDatabase() throws IOException, SQLException {
            EmbeddedPostgres pg = EmbeddedPostgres.start();

            DataSource dataSource1 = pg.getPostgresDatabase();

            sql = new Sql(dataSource1.getConnection());

            sql.execute(
                    "DROP TABLE IF EXISTS registry;" +
                            " CREATE TABLE registry(" +
                            "group_id VARCHAR PRIMARY KEY NOT NULL," +
                            "entry JSON NOT NULL," +
                            "version integer NOT NULL" +
                            ");"
            );

            dataSource = dataSource1;

            // insert 400 nodes with three random versions
            Map<String, NodeGroup> nodeGroupMap = new HashMap<>();

            IntStream.range(0, 399).forEachOrdered(nodeCounter -> {
                Node node = nodeWith("group-" + ThreadLocalRandom.current().nextInt(80));
                nodesMap.compute(node.getGroup(),
                        (group, nodeList) -> nodeList == null ? new ArrayList<>() : nodeList)
                        .add(node);
            });

            for (Map.Entry<String, List<Node>> entry : nodesMap.entrySet()) {
                sql.execute(
                        "INSERT INTO registry(group_id, entry, version) VALUES(?,?::JSON,?)",
                        new Object[]{
                                entry.getKey(),
                                new NodeGroup(entry.getValue()).nodesToJson(),
                                0});
            }
        }
    }

    private static Node nodeWith(String group) {
        try {
            return Node.builder()
                .localUrl(new URL("http://some.node.url"))
                .group(group)
                .status(Status.FOLLOWING)
                .offset(100)
                .following(Lists.newArrayList(new URL("http://some.node.url")))
                .lastSeen(ZonedDateTime.now())
                .requestedToFollow(Lists.newArrayList(new URL("http://some.node.url")))
                .pipe(Maps.of("v", randomVersion()))
                .build();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @State(Scope.Thread)
    public static class NodeState {

        private Node node;

        @Setup(Level.Invocation)
        public void doSetup() throws Exception {
            node = allNodes.get(ThreadLocalRandom.current().nextInt(allNodes.size()));
        }

        @TearDown(Level.Invocation)
        public void doTearDown() {
            node = null;
        }
    }

    @Benchmark
    public void registerNode(PostgresRegistry postgresRegistry, NodeState nodeState, Blackhole blackhole) {
        List<URL> hierarchyForRegisteredNode = postgresRegistry.registry.register(nodeState.node);
        blackhole.consume(hierarchyForRegisteredNode);
    }
}
