package com.tesco.aqueduct.registry.postgres;

import com.beust.jcommander.internal.Lists;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.registry.model.Status;
import groovy.sql.Sql;
import org.apache.groovy.util.Maps;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time=5)
@Measurement(iterations = 5, time = 2)
public class NodeRegistryBenchmark {

    private static List<String> versions = Lists.newArrayList("1.0","1.1","2.0");

    private static String randomVersion() {
        return versions.get(ThreadLocalRandom.current().nextInt(3));
    }

    @State(Scope.Benchmark)
    public static class PostgresRegistry {

        private EmbeddedPostgres pg;
        private NodeRegistry registry;
        private Sql sql;
        private URL cloudURL;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            cloudURL = new URL("http://cloud.pipe:8080");
            DataSource dataSource = setupDatabase();
            registry = new PostgreSQLNodeRegistry(dataSource, cloudURL, Duration.ofDays(1));
        }


        @TearDown(Level.Trial)
        public void doTearDown() {
            sql.close();
        }

        DataSource setupDatabase() throws SQLException, IOException {
            pg = EmbeddedPostgres.start();

            DataSource dataSource = pg.getPostgresDatabase();

            sql = new Sql(dataSource.getConnection());

            sql.execute(
                "DROP TABLE IF EXISTS registry;" +
                " CREATE TABLE registry(" +
                "group_id VARCHAR PRIMARY KEY NOT NULL," +
                "entry JSON NOT NULL," +
                "version integer NOT NULL" +
                ");"
            );

            return dataSource;
        }
    }

    @State(Scope.Thread)
    public static class NodeState {

        private Node node;

        @Setup(Level.Invocation)
        public void doSetup() throws Exception {
            ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
            String group = "group-" + threadLocalRandom.nextInt(10);

            node = Node.builder()
                .localUrl(new URL("http://some.node.url"))
                .group(group)
                .status(Status.FOLLOWING)
                .offset(100)
                .following(Lists.newArrayList(new URL("http://some.node.url")))
                .lastSeen(ZonedDateTime.now())
                .requestedToFollow(Lists.newArrayList(new URL("http://some.node.url")))
                .pipe(Maps.of("v", randomVersion()))
                .build();

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
