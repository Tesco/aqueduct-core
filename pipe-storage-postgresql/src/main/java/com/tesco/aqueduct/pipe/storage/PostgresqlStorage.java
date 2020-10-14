package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PostgresqlStorage implements CentralStorage {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));
    public static final String DEFAULT_CLUSTER = "NONE";

    private final int limit;
    private final DataSource dataSource;
    private final long maxBatchSize;
    private final long retryAfter;
    private final int readDelaySeconds;
    private final int nodeCount;
    private final long clusterDBPoolSize;
    private String currentTimestamp = "CURRENT_TIMESTAMP";
    private final int workMemMb;

    public PostgresqlStorage(
        final DataSource dataSource,
        final int limit,
        final long retryAfter,
        final long maxBatchSize,
        final int readDelaySeconds,
        int nodeCount,
        long clusterDBPoolSize,
        int workMemMb
    ) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.dataSource = dataSource;
        this.readDelaySeconds = readDelaySeconds;
        this.nodeCount = nodeCount;
        this.clusterDBPoolSize = clusterDBPoolSize;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);
        this.workMemMb = workMemMb;

        //initialise connection pool eagerly
        try (Connection connection = this.dataSource.getConnection()) {
            LOG.debug("postgresql storage", "initialised connection pool");
        } catch (SQLException e) {
            LOG.error("postgresql storage", "Error initializing connection pool", e);
        }
    }

    @Override
    public MessageResults read(
        final List<String> types,
        final long startOffset,
        final List<String> clusterUuids
    ) {
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection()) {
            LOG.info("getConnection:time", Long.toString(System.currentTimeMillis() - start));

            connection.setAutoCommit(false);
            setWorkMem(connection);

            final long globalLatestOffset = getLatestOffsetWithConnection(connection);

            final Array clusterIds = getClusterIds(connection, clusterUuids);

            try (PreparedStatement messagesQuery = getMessagesStatement(connection, types, startOffset, globalLatestOffset, clusterIds)) {

                final List<Message> messages = runMessagesQuery(messagesQuery);
                long end = System.currentTimeMillis();

                final long retry = calculateRetryAfter(end - start, messages.size());

                LOG.info("PostgresSqlStorage:retry", String.valueOf(retry));
                return new MessageResults(messages, retry, OptionalLong.of(globalLatestOffset), PipeState.UP_TO_DATE);
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
    }

    private Array getClusterIds(Connection connection, List<String> clusterUuids) {
        try(PreparedStatement statement = getClusterIdStatement(connection, clusterUuids)) {
            return runClusterIdsQuery(statement);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get cluster ids", exception);
            throw new RuntimeException(exception);
        }
    }

    // Setting work_mem here to avoid disk based sorts in Postgres
    private void setWorkMem(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getWorkMemQuery())) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long calculateRetryAfter(long queryTimeMs, int messagesCount) {
        if (messagesCount == 0) {
            return retryAfterWithRandomJitter();
        }

        if (queryTimeMs == 0) {
            return 1;
        }

        // retry after = readers / (connections / query time)
        final double dbThreshold = this.clusterDBPoolSize / (double) queryTimeMs;
        final double retryAfterMs = this.nodeCount / dbThreshold;
        final long calculatedRetryAfter = (long) Math.ceil(retryAfterMs);

        LOG.info("PostgresSqlStorage:calculateRetryAfter:messagesCount", String.valueOf(messagesCount));
        LOG.info("PostgresSqlStorage:calculateRetryAfter:calculatedRetryAfter", String.valueOf(calculatedRetryAfter));

        return Math.min(calculatedRetryAfter, retryAfter);
    }

    private long retryAfterWithRandomJitter() {
        return retryAfter + (long) (retryAfter * Math.random());
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        try (Connection connection = dataSource.getConnection()) {
            return OptionalLong.of(getLatestOffsetWithConnection(connection));
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void runVisibilityCheck() {
        try (Connection connection = dataSource.getConnection()) {
            Map<String, Long> messageCountByType = getMessageCountByType(connection);

            messageCountByType.forEach((key, value) ->
                LOG.info("count:type:" + key, String.valueOf(value))
            );
        } catch (SQLException exception) {
            LOG.error("postgres storage", "run visibility check", exception);
            throw new RuntimeException(exception);
        }
    }

    private Map<String, Long> getMessageCountByType(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(getMessageCountByTypeQuery())) {
            return runMessageCountByTypeQuery(statement);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getMessageCountByType:time", Long.toString(end - start));
        }
    }

    private Map<String, Long> runMessageCountByTypeQuery(final PreparedStatement preparedStatement) throws SQLException {
        Map<String, Long> messageCountByType = new HashMap<>();

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while(resultSet.next()) {
                final String type = resultSet.getString("type");
                final Long typeCount = resultSet.getLong("count");

                messageCountByType.put(type, typeCount);
            }
        }
        return messageCountByType;
    }

    private long getLatestOffsetWithConnection(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();

        try (PreparedStatement statement = getLatestOffsetStatement(connection);
            ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();

            return resultSet.getLong("last_offset");
        }finally {
            long end = System.currentTimeMillis();
            LOG.info("getLatestOffsetWithConnection:time", Long.toString(end - start));
        }
    }

    private List<Message> runMessagesQuery(final PreparedStatement query) throws SQLException {
        final List<Message> messages = new ArrayList<>();
        long start = System.currentTimeMillis();

        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                final String type = rs.getString("type");
                final String key = rs.getString("msg_key");
                final String contentType = rs.getString("content_type");
                final Long offset = rs.getLong("msg_offset");
                final ZonedDateTime created = ZonedDateTime.of(rs.getTimestamp("created_utc").toLocalDateTime(), ZoneId.of("UTC"));
                final String data = rs.getString("data");

                messages.add(new Message(type, key, contentType, offset, created, data));
            }
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runMessagesQuery:time", Long.toString(end - start));
        }
        return messages;
    }

    private Array runClusterIdsQuery(final PreparedStatement query) throws SQLException {
        long start = System.currentTimeMillis();
        try (ResultSet rs = query.executeQuery()) {
            rs.next();
            return rs.getArray(1);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runClusterIdsQuery:time", Long.toString(end - start));
        }
    }

    private PreparedStatement getLatestOffsetStatement(final Connection connection) {
        try {
            return connection.prepareStatement(getSelectLatestOffsetQuery());
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private PreparedStatement getClusterIdStatement(final Connection connection, final List<String> clusterUuids) {
        try {
            final String strClusters = String.join(",", clusterUuidsWithDefaultCluster(clusterUuids));
            PreparedStatement query = connection.prepareStatement(getClusterIdQuery());
            query.setString(1, strClusters);
            return query;
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get cluster ids statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private PreparedStatement getMessagesStatement(
        final Connection connection,
        final List<String> types,
        final long startOffset,
        long endOffset,
        final Array clusterIds
    ) {
        try {
            PreparedStatement query;

            if (types == null || types.isEmpty()) {
                query = connection.prepareStatement(getSelectEventsWithoutTypeQuery(maxBatchSize));
                query.setArray(1,clusterIds );
                query.setLong(2, startOffset);
                query.setLong(3, endOffset);
                query.setLong(4, limit);
            } else {
                final String strTypes = String.join(",", types);
                query = connection.prepareStatement(getSelectEventsWithTypeQuery(maxBatchSize));
                query.setArray(1, clusterIds);
                query.setLong(2, startOffset);
                query.setLong(3, endOffset);
                query.setString(4, strTypes);
                query.setLong(5, limit);
            }

            return query;

        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get message statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private List<String> clusterUuidsWithDefaultCluster(List<String> clusterUuids) {
        // It is not safe to assume that clusterUuids is a mutable list when its not created here, hence create a copy
        // before adding default cluster to it
        final List<String> clusterUuidsWithDefaultCluster = new ArrayList<>(clusterUuids);
        clusterUuidsWithDefaultCluster.add(DEFAULT_CLUSTER);
        return Collections.unmodifiableList(clusterUuidsWithDefaultCluster);
    }

    public void compactUpTo(final ZonedDateTime thresholdDate) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(getCompactionQuery())) {
                Timestamp threshold = Timestamp.valueOf(thresholdDate.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
                statement.setTimestamp(1, threshold);
                statement.setTimestamp(2, threshold);
                final int rowsAffected = statement.executeUpdate();
                LOG.info("compaction", "compacted " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void vacuumAnalyseEvents() {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(getVacuumAnalyseQuery())) {
            statement.executeUpdate();
            LOG.info("vacuum analyse", "vacuum analyse complete");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSelectEventsWithoutTypeQuery(long maxBatchSize) {
        return
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            " ( " +
            "   SELECT " +
            "     type, msg_key, content_type, msg_offset, created_utc, data, " +
            "     SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "   FROM events " +
                  addClusterFilter() +
            "   AND events.msg_offset >= ? " +
            "   AND events.msg_offset <= ?" +
            " ORDER BY msg_offset " +
            " LIMIT ?" +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    private String getSelectEventsWithTypeQuery(long maxBatchSize) {
        return
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            " ( " +
            "   SELECT " +
            "     type, msg_key, content_type, msg_offset, created_utc, data, " +
            "     SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "   FROM events " +
                    addClusterFilter() +
            "   AND events.msg_offset >= ? " +
            "   AND events.msg_offset <= ?" +
            "   AND type = ANY (string_to_array(?, ','))" +
            " ORDER BY msg_offset " +
            " LIMIT ?" +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    private String addClusterFilter() {
        return
            " WHERE " +
            " cluster_id = ANY (?) ";
    }

    private String getClusterIdQuery() {
        return "SELECT array_agg(cluster_id) FROM clusters WHERE ((cluster_uuid)::text = ANY (string_to_array(?, ',')));";
    }

    private String getSelectLatestOffsetQuery() {
        String filterCondition = "WHERE created_utc >= %s - INTERVAL '%s SECONDS'";
        return
            "SELECT coalesce (" +
                " (SELECT min(msg_offset) - 1 from events where msg_offset in " +
                "   (SELECT msg_offset FROM events " + String.format(filterCondition, currentTimestamp, readDelaySeconds) + ")" +
                " ), " +
                " (SELECT max(msg_offset) FROM events), " +
                " 0 " +
            ") as last_offset;";
    }

    private static String getCompactionQuery() {
        return
            " DELETE FROM events " +
            " WHERE msg_offset in " +
            " (" +
            "   SELECT msg_offset " +
            "   FROM " +
            "   events e, " +
            "   ( " +
            "     SELECT msg_key, cluster_id, max(msg_offset) max_offset, type" +
            "     FROM events " +
            "     WHERE " +
            "       created_utc <= ? " +
            "     GROUP BY msg_key, cluster_id, type" +
            "   ) x " +
            "   WHERE " +
            "     created_utc <= ? " +
            "     AND e.msg_key = x.msg_key " +
            "     AND e.cluster_id = x.cluster_id " +
            "     AND e.type = x.type" +
            "     AND e.msg_offset <> x.max_offset " +
            " );";
    }

    private static String getVacuumAnalyseQuery() {
        return
            " VACUUM ANALYSE EVENTS; " +
            " VACUUM ANALYSE CLUSTERS; " +
            " VACUUM ANALYSE REGISTRY; " +
            " VACUUM ANALYSE NODE_REQUESTS; ";
    }

    private String getWorkMemQuery() {
        return " SET LOCAL work_mem TO '" + workMemMb + "MB';";
    }
    
    private static String getMessageCountByTypeQuery() {
        return "SELECT type, COUNT(type) FROM events GROUP BY type;";
    }
}
