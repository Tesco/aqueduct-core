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

    private final int limit;
    private final DataSource pipeDataSource;
    private final DataSource compactionDataSource;
    private final long maxBatchSize;
    private final long retryAfter;
    private final OffsetFetcher offsetFetcher;
    private final int nodeCount;
    private final long clusterDBPoolSize;
    private final int workMemMb;
    private ClusterStorage clusterStorage;

    public PostgresqlStorage(
        final DataSource pipeDataSource,
        final DataSource compactionDataSource,
        final int limit,
        final long retryAfter,
        final long maxBatchSize,
        final OffsetFetcher offsetFetcher,
        int nodeCount,
        long clusterDBPoolSize,
        int workMemMb,
        ClusterStorage clusterStorage
    ) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.pipeDataSource = pipeDataSource;
        this.compactionDataSource = compactionDataSource;
        this.offsetFetcher = offsetFetcher;
        this.nodeCount = nodeCount;
        this.clusterDBPoolSize = clusterDBPoolSize;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);
        this.workMemMb = workMemMb;
        this.clusterStorage = clusterStorage;

        //initialise connection pool eagerly
        try (Connection connection = this.pipeDataSource.getConnection()) {
            LOG.debug("postgresql storage", "initialised connection pool");
        } catch (SQLException e) {
            LOG.error("postgresql storage", "Error initializing connection pool", e);
        }
    }

    @Override
    public MessageResults read(
        final List<String> types,
        final long startOffset,
        final String locationUuid
    ) {
        long start = System.currentTimeMillis();
        Connection connection = null;
        try {
            connection = getConnection();

            final Optional<ClusterCacheEntry> entry = clusterStorage.getClusterCacheEntry(locationUuid, connection);

            final List<Long> locationGroups = getLocationGroupsFor(locationUuid, connection);

            if (isValidAndUnexpired(entry)) {
                return readMessages(types, start, startOffset, entry.get().getClusterIds(), locationGroups, connection);

            } else {
                close(connection);

                final List<String> clusterUuids = clusterStorage.resolveClustersFor(locationUuid);

                connection = getConnection();

                final Optional<List<Long>> newClusterIds = clusterStorage.updateAndGetClusterIds(locationUuid, clusterUuids, entry, connection);

                if (newClusterIds.isPresent()) {
                    return readMessages(types, start, startOffset, newClusterIds.get(), locationGroups, connection);
                } else {
                    LOG.info("postgresql storage", "Recursive read due to Cluster Cache invalidation race condition");
                    return read(types, startOffset, locationUuid);
                }
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            throw new RuntimeException(exception);
        } finally {
            if (connection != null) {
                close(connection);
            }
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
    }

    private List<Long> getLocationGroupsFor(String locationUuid, Connection connection) {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(getSelectLocationGroupsQuery())) {
            statement.setString(1, locationUuid);
            ResultSet resultSet = statement.executeQuery();

            if(resultSet.next()) {
                Array groups = resultSet.getArray("groups");
                return Arrays.asList( (Long[]) groups.getArray());
            } else {
                return Collections.emptyList();
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "resolve groups for location", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getLocationGroupsFor:time", Long.toString(end - start));
        }
    }

    private String getSelectLocationGroupsQuery() {
        return "SELECT groups FROM LOCATION_GROUPS WHERE location_uuid = ?;";
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = pipeDataSource.getConnection();
        LOG.info("getConnection:time", Long.toString(System.currentTimeMillis() - start));
        return connection;
    }

    private boolean isValidAndUnexpired(Optional<ClusterCacheEntry> entry) {
        return entry.map(ClusterCacheEntry::isValidAndUnexpired).orElse(false);
    }

    private MessageResults readMessages(
        List<String> types,
        long start,
        long startOffset,
        List<Long> clusterIds,
        List<Long> locationGroups,
        Connection connection
    ) throws SQLException {

        connection.setAutoCommit(false);
        setWorkMem(connection);

        final long globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection);

        try (PreparedStatement messagesQuery = getMessagesStatement(connection, types, startOffset, globalLatestOffset, clusterIds, locationGroups)) {

            final List<Message> messages = runMessagesQuery(messagesQuery);
            long end = System.currentTimeMillis();

            final long retry = calculateRetryAfter(end - start, messages.size());

            LOG.info("PostgresSqlStorage:retry", String.valueOf(retry));
            return new MessageResults(messages, retry, OptionalLong.of(globalLatestOffset), PipeState.UP_TO_DATE);
        }
    }

    private void close(Connection connection) {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "close", exception);
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
        try (Connection connection = pipeDataSource.getConnection()) {
            return OptionalLong.of(offsetFetcher.getGlobalLatestOffset(connection));
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public long getOffsetConsistencySum(long offset, List<String> targetUuids) {
        throw new UnsupportedOperationException("Offset consistency sum isn't implemented yet");
    }

    @Override
    @Deprecated
    public void runVisibilityCheck() {
        try (Connection connection = compactionDataSource.getConnection()) {
            Map<String, Long> messageCountByType = getMessageCountByType(connection);

            messageCountByType.forEach((key, value) ->
                LOG.info("count:type:" + key, String.valueOf(value))
            );
        } catch (SQLException exception) {
            LOG.error("postgres storage", "run visibility check", exception);
            throw new RuntimeException(exception);
        }
    }

    private void runVisibilityCheck(Connection connection) {
        try {
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

    private List<Message> runMessagesQuery(final PreparedStatement query) throws SQLException {
        final List<Message> messages = new ArrayList<>();
        long start = System.currentTimeMillis();

        try (ResultSet rs = query.executeQuery()) {
            long startProcessingResults = System.currentTimeMillis();
            while (rs.next()) {
                final String type = rs.getString("type");
                final String key = rs.getString("msg_key");
                final String contentType = rs.getString("content_type");
                final Long offset = rs.getLong("msg_offset");
                final ZonedDateTime created = ZonedDateTime.of(rs.getTimestamp("created_utc").toLocalDateTime(), ZoneId.of("UTC"));
                final String data = rs.getString("data");

                messages.add(new Message(type, key, contentType, offset, created, data));
            }

            LOG.info("runMessagesQuery:time processing results", Long.toString(System.currentTimeMillis() - startProcessingResults));
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runMessagesQuery:time", Long.toString(end - start));
        }
        return messages;
    }

    private PreparedStatement getMessagesStatement(
        final Connection connection,
        final List<String> types,
        final long startOffset,
        long endOffset,
        final List<Long> clusterIds,
        List<Long> locationGroups) {
        try {
            PreparedStatement query;

            final Array clusterIdArray = connection.createArrayOf("BIGINT", clusterIds.toArray());
            final Array locationGroupsArray = connection.createArrayOf("BIGINT", locationGroups.toArray());

            if (types == null || types.isEmpty()) {
                query = connection.prepareStatement(getSelectEventsWithoutTypeQuery(maxBatchSize));
                query.setArray(1, clusterIdArray);
                query.setArray(2, locationGroupsArray);
                query.setLong(3, startOffset);
                query.setLong(4, endOffset);
                query.setLong(5, limit);
            } else {
                final String strTypes = String.join(",", types);
                query = connection.prepareStatement(getSelectEventsWithTypeQuery(maxBatchSize));
                query.setArray(1, clusterIdArray);
                query.setArray(2, locationGroupsArray);
                query.setLong(3, startOffset);
                query.setLong(4, endOffset);
                query.setString(5, strTypes);
                query.setLong(6, limit);
            }

            return query;

        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get message statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private void compact(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getCompactionQuery())) {
            final int rowsAffected = statement.executeUpdate();
            LOG.info("compaction", "compacted " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean compactAndMaintain() {
        boolean compacted = false;
        try (Connection connection = compactionDataSource.getConnection()) {
            connection.setAutoCommit(false);
            if(attemptToLock(connection)){
                compacted=true;
                LOG.info("compact and maintain", "obtained lock, compacting");
                compact(connection);
                runVisibilityCheck(connection);

                //start a new transaction for vacuuming
                connection.commit();
                connection.setAutoCommit(true);

                vacuumAnalyseEvents(connection);
            } else {
                LOG.info("compact and maintain", "didn't obtain lock");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return compacted;
    }

    private boolean attemptToLock(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getLockingQuery())) {
            return statement.execute();
        } catch (SQLException e) {
            if(e.getSQLState().equals("55P03")) {
                //lock was not available
                return false;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void vacuumAnalyseEvents(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getVacuumAnalyseQuery())) {
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
                  addClusterAndLocationGroupFilter() +
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
                    addClusterAndLocationGroupFilter() +
            "   AND events.msg_offset >= ? " +
            "   AND events.msg_offset <= ?" +
            "   AND type = ANY (string_to_array(?, ','))" +
            " ORDER BY msg_offset " +
            " LIMIT ?" +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    private String addClusterAndLocationGroupFilter() {
        return
            " WHERE " +
            " cluster_id = ANY (?) AND (location_group IS NULL OR location_group = ANY (?)) ";
    }

    private static String getCompactionQuery() {
        return "DELETE FROM events WHERE time_to_live < CURRENT_TIMESTAMP;";
    }

    private static String getVacuumAnalyseQuery() {
        return
            " VACUUM ANALYSE EVENTS; " +
            " VACUUM ANALYSE EVENTS_BUFFER; " +
            " VACUUM ANALYSE CLUSTERS; " +
            " VACUUM ANALYSE REGISTRY; " +
            " VACUUM ANALYSE NODE_REQUESTS; ";
    }

    private String getWorkMemQuery() {
        return " SET LOCAL work_mem TO '" + workMemMb + "MB';";
    }

    private String getLockingQuery() {
        return "SELECT * from locks where name='maintenance_lock' FOR UPDATE NOWAIT;";
    }

    private static String getMessageCountByTypeQuery() {
        return "SELECT type, COUNT(type) FROM events GROUP BY type;";
    }
}
