package com.tesco.aqueduct.pipe.storage;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageResults;
import groovy.sql.Sql;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Fork(value = 1)
@Measurement(iterations = 10, time = 30)
public class ReadEventsQueryBenchmark {

    static List<Long> clusterIds;

    static List<String> types;

    static Map<Long, String> clusterMap = new HashMap<>();

    public static final String TYPE_PREFIX = "Type_";

    private static String typeName(int i) {
        return TYPE_PREFIX + i;
    }

    private static Long randomClusterId() {
        return clusterIds.get(ThreadLocalRandom.current().nextInt(100));
    }

    private static String randomType() {
        return types.get(ThreadLocalRandom.current().nextInt(10));
    }

    private static String randomClusterUuid() {
        return clusterMap.get(randomClusterId());
    }

    @State(Scope.Benchmark)
    public static class PostgresDatabaseState {

        public static final int CLUSTER_COUNT = 100;
        public static final int TYPES_COUNT = 10;
        private EmbeddedPostgres pg;
        private PostgresqlStorage storage;
        private Sql sql;

        private long retryAfter = 5000;
        private int limit = 20000;
        private long batchSize = 2000000;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            System.out.println("setUp invoked");

            setupDatabase();

            clusterIds = new ArrayList<>(CLUSTER_COUNT);
            types = new ArrayList<>(TYPES_COUNT);

            for (int i=0; i < TYPES_COUNT; i++) {
                types.add(typeName(i));
            }

            for (int i=0; i<CLUSTER_COUNT; i++) {
                String clusterUuid = "Cluster_" + i;
                Long clusterId = insertCluster(clusterUuid);
                clusterIds.add(clusterId);
                clusterMap.put(clusterId, clusterUuid);
            }

            for (long i=0; i<1000000; i++) {
                insertWithCluster(
                    message(i, randomType(), "key_" + i, "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), MESSAGE_CONTENT),
                    randomClusterId());
            }
        }


        @TearDown(Level.Trial)
        public void doTearDown() {
            System.out.println("teardown invoked");

            sql.close();
        }

        void setupDatabase() throws SQLException, IOException {
            pg = EmbeddedPostgres.start();

            DataSource dataSource = pg.getPostgresDatabase();

            sql = new Sql(dataSource.getConnection());

            sql.execute(
            "DROP TABLE IF EXISTS EVENTS;" +
                "DROP TABLE IF EXISTS CLUSTERS;" +
                "CREATE TABLE EVENTS(" +
                "    msg_offset BIGSERIAL PRIMARY KEY NOT NULL," +
                "    msg_key varchar NOT NULL," +
                "    content_type varchar NOT NULL," +
                "    type varchar NOT NULL," +
                "    created_utc timestamp NOT NULL," +
                "    data text NULL," +
                "    event_size int NOT NULL," +
                "    cluster_id BIGINT NOT NULL DEFAULT 1," +
                "    time_to_live TIMESTAMP NULL" +
                ");" +
                "CREATE TABLE CLUSTERS(" +
                "    cluster_id BIGSERIAL PRIMARY KEY NOT NULL," +
                "    cluster_uuid VARCHAR NOT NULL" +
                ");" +
                "INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');" +
                "CREATE INDEX type_idx ON EVENTS (type);" +
                "CREATE INDEX key_idx ON EVENTS (msg_key);" +
                "CREATE INDEX created_idx ON EVENTS (created_utc, msg_key);" +
                "CREATE INDEX cluster_idx ON EVENTS (msg_key, cluster_id);" +
                "CREATE INDEX cluster_type_filter_idx ON EVENTS (type, cluster_id);" +
                "CREATE INDEX cluster_only_idx ON EVENTS (cluster_id);" +
                "CREATE INDEX cluster_uuid_idx ON CLUSTERS (cluster_uuid);"
            );

            storage = new PostgresqlStorage(dataSource, dataSource, limit, retryAfter, batchSize, 0, 1, 1, 4);
        }

        void insertWithCluster(Message msg, Long clusterId) throws SQLException {
            Timestamp time = Timestamp.valueOf(msg.getCreated().toLocalDateTime());
            sql.execute(
                    "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
                    new Object[] {msg.getOffset(), msg.getKey(), msg.getContentType(), msg.getType(), time, msg.getData(), 0, clusterId});
        }

        Long insertCluster(String clusterUuid) throws SQLException {
            return (Long)sql.executeInsert("INSERT INTO CLUSTERS(cluster_uuid) VALUES (?);", new Object[] {clusterUuid}).get(0).get(0);
        }

        Message message(Long offset, String type, String key, String contentType, ZonedDateTime created, String data) {
            return new Message(
                type,
                key,
                contentType,
                offset,
                created,
                data
            );
        }
    }

    @State(Scope.Benchmark)
    public static class FilterState {

        List<String> clustersToFilter;

        List<String> typesToFilter;

        @Setup(Level.Invocation)
        public void doSetup() {
            ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

            clustersToFilter = IntStream.range(0, threadLocalRandom.nextInt(10) + 1)
                    .mapToObj(i -> randomClusterUuid()).collect(Collectors.toList());

            Collections.shuffle(clustersToFilter);

            typesToFilter = IntStream.range(0, threadLocalRandom.nextInt(PostgresDatabaseState.TYPES_COUNT) + 1)
                    .mapToObj(ReadEventsQueryBenchmark::typeName).collect(Collectors.toList());

            Collections.shuffle(typesToFilter);
        }

        @TearDown(Level.Invocation)
        public void doTearDown() {
            typesToFilter = null;
            clustersToFilter = null;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void eventsQuery(PostgresDatabaseState postgresDatabaseState, FilterState filterState, Blackhole blackhole) {
        MessageResults messageResults = postgresDatabaseState.storage.read(filterState.typesToFilter, 0, filterState.clustersToFilter);
        blackhole.consume(messageResults);
    }

    private static final String MESSAGE_CONTENT = "{\n" +
            "  \"data\" : [\n" +
            "    {\n" +
            "      \"id\": \"DATA1\",\n" +
            "      \"bucketNumber\": \"790\",\n" +
            "      \"dataGroups\": [\n" +
            "        \"D1\"\n" +
            "      ],\n" +
            "      \"startDateTime\": \"2017-04-19T00:00:00+00:00\",\n" +
            "      \"endDateTime\": \"2030-07-11T23:59:59+00:00\",\n" +
            "      \"name\": \"data\",\n" +
            "      \"shortDescription\": \"data\",\n" +
            "      \"condition\": {\n" +
            "        \"id\": \"1\",\n" +
            "        \"type\": \"dataMatch\",\n" +
            "        \"data\": [\n" +
            "          \"05050179865189\",\n" +
            "          \"00111122223333\"\n" +
            "        ],\n" +
            "        \"requiredQuantityMin\": \"1\",\n" +
            "        \"requiredQuantityMax\": \"1\",\n" +
            "        \"cheapest\": true\n" +
            "      },\n" +
            "      \"dataRules\": [\n" +
            "        {\n" +
            "          \"applyTo\": {\n" +
            "            \"conditions\": [\n" +
            "              \"1\"\n" +
            "            ],\n" +
            "            \"cheapest\": true,\n" +
            "            \"maxQuantity\": \"1\"\n" +
            "          },\n" +
            "          \"type\": \"percentageData\",\n" +
            "          \"percentageData\": \"20\",\n" +
            "          \"addToData\" : true\n" +
            "        }\n" +
            "      ],\n" +
            "      \"type\": \"data\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"dataGroups\": [\n" +
            "    {\n" +
            "      \"id\": \"A1\",\n" +
            "      \"data\": [\n" +
            "        \"02065\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

}
