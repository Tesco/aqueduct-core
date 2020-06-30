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

    public PostgresqlStorage(final DataSource dataSource, final int limit, final long retryAfter, final long maxBatchSize) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.dataSource = dataSource;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);
    }

    @Override
    public MessageResults read(
        final List<String> types,
        final long startOffset,
        final List<String> clusterUuids) {

        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection();
            PreparedStatement messagesQuery = getMessagesStatement(connection, types, startOffset, clusterUuids)) {

            final long globalLatestOffset = getLatestOffsetWithConnection(connection);
            final long retry = startOffset >= globalLatestOffset ? retryAfter : 0;

            LOG.withTypes(types).debug("postgresql storage", "reading with types");

            final List<Message> messages = runMessagesQuery(messagesQuery);

            return new MessageResults(messages, retry, OptionalLong.of(globalLatestOffset), PipeState.UP_TO_DATE);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
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
                LOG.withTypes(Collections.singletonList(key)).info("count:type", String.valueOf(value))
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

    private PreparedStatement getLatestOffsetStatement(final Connection connection) {
        try {
            return connection.prepareStatement(getSelectLatestOffsetQuery());
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private PreparedStatement getMessagesStatement(
        final Connection connection, final List<String> types, final long startOffset, final List<String> clusterUuids) {

        try {
            PreparedStatement query;

            final String strClusters = String.join(",", clusterUuidsWithDefaultCluster(clusterUuids));

            if (types == null || types.isEmpty()) {
                query = connection.prepareStatement(getSelectEventsWithoutTypeQuery(maxBatchSize));
                query.setString(1, strClusters);
                query.setLong(2, startOffset);
                query.setLong(3, limit);
            } else {
                final String strTypes = String.join(",", types);
                query = connection.prepareStatement(getSelectEventsWithTypeQuery(maxBatchSize));
                query.setString(1, strClusters);
                query.setLong(2, startOffset);
                query.setString(3, strTypes);
                query.setLong(4, limit);
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
        try(Connection connection = dataSource.getConnection();
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

    private String getSelectEventsWithoutTypeQuery(long maxBatchSize) {
        return
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            " ( " +
            "   SELECT " +
            "     type, msg_key, content_type, msg_offset, created_utc, data, " +
            "     SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "   FROM events " +
                  withInnerJoinToClusters() +
            "   AND events.msg_offset >= ? " +
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
                  withInnerJoinToClusters() +
            "   AND events.msg_offset >= ? " +
            "   AND type = ANY (string_to_array(?, ','))" +
            " ORDER BY msg_offset " +
            " LIMIT ?" +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    private String withInnerJoinToClusters() {
        return
            " INNER JOIN clusters ON (events.cluster_id = clusters.cluster_id)" +
            " WHERE " +
            " clusters.cluster_uuid = ANY (string_to_array(?, ',')) ";
    }

    private static String getSelectLatestOffsetQuery() {
        return " SELECT coalesce(max(msg_offset),0) as last_offset FROM events ;";
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
            "     SELECT msg_key, cluster_id, max(msg_offset) max_offset " +
            "     FROM events " +
            "     WHERE " +
            "       created_utc <= ? " +
            "     GROUP BY msg_key, cluster_id" +
            "   ) x " +
            "   WHERE " +
            "     created_utc <= ? " +
            "     AND e.msg_key = x.msg_key AND e.cluster_id = x.cluster_id AND e.msg_offset <> x.max_offset " +
            " );";
    }
    
    private static String getMessageCountByTypeQuery() {
        return "SELECT type, COUNT(type) FROM events GROUP BY type;";
    }
}
