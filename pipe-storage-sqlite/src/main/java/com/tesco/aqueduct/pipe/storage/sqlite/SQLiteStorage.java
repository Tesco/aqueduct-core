package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET;

public class SQLiteStorage implements DistributedStorage {

    private final DataSource dataSource;
    private final int limit;
    private final int retryAfterMs;
    private final long maxBatchSize;

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(SQLiteStorage.class));

    public SQLiteStorage(final DataSource dataSource, final int limit, final int retryAfterMs, final long maxBatchSize) {
        this.dataSource = dataSource;
        this.limit = limit;
        this.retryAfterMs = retryAfterMs;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);

        createEventTableIfNotExists();
        createOffsetTableIfNotExists();
        createPipeStateTableIfNotExists();
    }

    private void createEventTableIfNotExists() {
        execute(
            SQLiteQueries.CREATE_EVENT_TABLE,
            (connection, statement) -> statement.execute()
        );
    }

    private void createOffsetTableIfNotExists() {
        execute(
            SQLiteQueries.OFFSET_TABLE,
            (connection, statement) -> statement.execute()
        );
    }

    private void createPipeStateTableIfNotExists() {
        execute(
            SQLiteQueries.PIPE_STATE_TABLE,
            (Connection, statement) -> statement.execute()
        );
    }

    @Override
    public MessageResults read(final List<String> types, final long offset, final String locationUuid) {
        final List<Message> retrievedMessages = new ArrayList<>();
        final int typesCount = types == null ? 0 : types.size();

        /*
         * Assumption is that reading offset and state before messages will be consistent, could be wrong
         * We think this is better than before, but needs more investigation in the future
         */
        OptionalLong globalLatestOffset = getOffset(GLOBAL_LATEST_OFFSET);
        PipeState pipeState = getPipeState();

        execute(
            SQLiteQueries.getReadEvent(typesCount, maxBatchSize),
            (connection, statement) -> {
                int parameterIndex = 1;
                statement.setLong(parameterIndex++, offset);

                for (int i = 0; i < typesCount; i++, parameterIndex++) {
                    statement.setString(parameterIndex, types.get(i));
                }

                statement.setLong(parameterIndex, limit);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        retrievedMessages.add(mapRetrievedMessageFromResultSet(resultSet));
                    }
                }
            }
        );

        return new MessageResults(retrievedMessages, calculateRetryAfter(retrievedMessages.size()), globalLatestOffset, pipeState);
    }

    @Override
    public PipeState getPipeState() {
        return executeGet(
            SQLiteQueries.GET_PIPE_STATE,
            (connection, statement) -> {
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next()
                        ? PipeState.valueOf(resultSet.getString("value"))
                        : PipeState.UNKNOWN;
                }
            }
        );
    }

    @Override
    public long getOffsetConsistencySum(long offset, List<String> targetUuids) {
        try (Connection connection = dataSource.getConnection()) {
            return getOffsetConsistencySumBasedOn(offset, connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int calculateRetryAfter(final int messageCount) {
        return messageCount > 0 ? 0 : retryAfterMs;
    }

    private Message mapRetrievedMessageFromResultSet(final ResultSet resultSet) throws SQLException {
        Message retrievedMessage;
        final ZonedDateTime time = ZonedDateTime.of(
            resultSet.getTimestamp("created_utc").toLocalDateTime(),
            ZoneId.of("UTC")
        );

        retrievedMessage = new Message(
            resultSet.getString("type"),
            resultSet.getString("msg_key"),
            resultSet.getString("content_type"),
            resultSet.getLong("msg_offset"),
            time,
            resultSet.getString("data"),
            resultSet.getLong("event_size")
        );

        return retrievedMessage;
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        if(offsetName == OffsetName.MAX_OFFSET_PREVIOUS_HOUR) {
            return getMaxOffsetInPreviousHour(ZonedDateTime.now());
        }

        return executeGet(
            SQLiteQueries.getOffset(offsetName),
            (connection, statement) -> {
                ResultSet resultSet = statement.executeQuery();

                return resultSet.next() ?
                    OptionalLong.of(resultSet.getLong("value")) : OptionalLong.empty();
            }
        );
    }

    @Override
    public void write(final Iterable<Message> messages) {
        execute(SQLiteQueries.INSERT_EVENT,
            (connection, statement) -> {
                connection.setAutoCommit(false);
                insertMessagesAsBatch(statement, messages);
                connection.commit();
            });
    }

    @Override
    public void write(final PipeEntity pipeEntity) {
        if (pipeEntity == null || nothingToWriteIn(pipeEntity)) {
            throw new IllegalArgumentException("Pipe entity data cannot be null.");
        }

        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            try (final PreparedStatement insertMessageStmt = connection.prepareStatement(SQLiteQueries.INSERT_EVENT);
                 final PreparedStatement upsertOffsetStmt = connection.prepareStatement(SQLiteQueries.UPSERT_OFFSET);
                 final PreparedStatement upsertPipeStateStmt = connection.prepareStatement(SQLiteQueries.UPSERT_PIPE_STATE)
            ) {
                // Start transaction
                connection.setAutoCommit(false);

                // Insert messages
                if (pipeEntity.getMessages() != null && !pipeEntity.getMessages().isEmpty()) {
                    insertMessagesAsBatch(insertMessageStmt, pipeEntity.getMessages());
                }

                // Insert offsets
                if (pipeEntity.getOffsets() != null && !pipeEntity.getOffsets().isEmpty()) {
                    upsertOffsetsAsBatch(upsertOffsetStmt, pipeEntity.getOffsets());
                }

                // Insert pipe state
                if (pipeEntity.getPipeState() != null) {
                    upsertPipeState(upsertPipeStateStmt, pipeEntity.getPipeState());
                }

                // commit transaction
                connection.commit();
            }
        } catch (final Exception exception) { // Catch all exceptions so that data is rolled back and connection's mode is reset
            rollback(connection);
            throw new RuntimeException(exception);

        } finally {
            close(connection);
        }
    }

    private void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException sqlException) {
            LOG.error("write", "Error while closing connection", sqlException);
            throw new RuntimeException(sqlException);
        }
    }

    private boolean nothingToWriteIn(PipeEntity pipeEntity) {
        return pipeEntity.getPipeState() == null
                && (pipeEntity.getOffsets() == null || pipeEntity.getOffsets().isEmpty())
                && (pipeEntity.getMessages() == null || pipeEntity.getMessages().isEmpty());
    }

    private void upsertPipeState(PreparedStatement upsertPipeStateStmt, PipeState pipeState) throws SQLException {
        upsertPipeStateStmt.setString(1, "pipe_state");
        upsertPipeStateStmt.setString(2, pipeState.toString());
        upsertPipeStateStmt.setString(3, pipeState.toString());
        upsertPipeStateStmt.execute();
    }

    private void rollback(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                }
            } catch (SQLException sqlException) {
                LOG.error("write", "Could not rollback pipe entity transaction.", sqlException);
                throw new RuntimeException(sqlException);
            }
        }
    }

    private void upsertOffsetsAsBatch(PreparedStatement insertOffsetStmt, List<OffsetEntity> offsets) throws SQLException {
        for (final OffsetEntity offset : offsets) {
            setStatementParametersForOffsetQuery(insertOffsetStmt, offset);
            insertOffsetStmt.addBatch();
        }
        insertOffsetStmt.executeBatch();
    }

    private void insertMessagesAsBatch(PreparedStatement insertMessageStmt, Iterable<Message> messages) throws SQLException {
        for (final Message message : messages) {
            setStatementParametersForInsertMessageQuery(insertMessageStmt, message);
            insertMessageStmt.addBatch();
        }
        insertMessageStmt.executeBatch();
    }

    private void setStatementParametersForOffsetQuery(PreparedStatement insertOffsetStmt, OffsetEntity offset) throws SQLException {
        insertOffsetStmt.setString(1, offset.getName().toString());
        insertOffsetStmt.setLong(2, offset.getValue().getAsLong());
        insertOffsetStmt.setLong(3, offset.getValue().getAsLong());
    }

    @Override
    public void write(final Message message) {
        execute(
            SQLiteQueries.INSERT_EVENT,
            (connection, statement) -> {
                setStatementParametersForInsertMessageQuery(statement, message);
                statement.execute();
            }
        );
    }

    @Override
    public void write(OffsetEntity offset) {
        execute(
            SQLiteQueries.UPSERT_OFFSET,
            (connection, statement) -> {
                setStatementParametersForOffsetQuery(statement, offset);
                statement.execute();
            }
        );
    }

    @Override
    public void write(PipeState pipeState) {
        execute(
            SQLiteQueries.UPSERT_PIPE_STATE,
            ((connection, statement) -> {
                upsertPipeState(statement, pipeState);
            })
        );
    }

    @Override
    public void runVisibilityCheck() {
        runIntegrityCheck();
    }

    private void runIntegrityCheck() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SQLiteQueries.INTEGRITY_CHECK)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                String result = resultSet.getString(1);
                if (!result.equals("ok")) {
                    LOG.error("integrity check", "integrity check failed", result);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getOffsetConsistencySumBasedOn(long offsetThreshold, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.OFFSET_CONSISTENCY_SUM)) {
            statement.setLong(1, offsetThreshold);
            return queryResult(statement);
        }
    }

    private OptionalLong getMaxOffsetInPreviousHour(ZonedDateTime currentTime) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CHOOSE_MAX_OFFSET)) {
            Timestamp threshold = Timestamp.valueOf(currentTime.withMinute(0).withSecond(0).withNano(0).toLocalDateTime());
            statement.setTimestamp(1, threshold);
            return OptionalLong.of(queryResult(statement));
        } catch (SQLException exception){
            throw new RuntimeException(exception);
        }
    }

    private long queryResult(PreparedStatement statement) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            return resultSet.getLong(1);
        }
    }

    private void execute(String query, SqlConsumer consumer) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            consumer.accept(connection, statement);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private <T> T executeGet(String query, SqlFunction<T> function) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            return function.apply(connection, statement);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private interface SqlConsumer {
        void accept(Connection connection, PreparedStatement statement) throws SQLException;
    }

    private interface SqlFunction<T> {
        T apply(Connection connection, PreparedStatement statement) throws SQLException;
    }

    @Override
    public void deleteAll() {
        try (Connection connection = dataSource.getConnection()){
            deleteEvents(connection);
            deleteOffsets(connection);
            deletePipeState(connection);
            vacuumDatabase(connection);
            checkpointWalFile(connection);
            shrinkMemory(connection);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    public void runMaintenanceTasks() {
        try (Connection connection = dataSource.getConnection()) {
            shrinkMemory(connection);
            vacuumDatabase(connection);
            checkpointWalFile(connection);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private void deleteOffsets(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_OFFSETS)) {
            statement.execute();
            LOG.info("deleteOffsets", String.format("Delete offsets result: %d", statement.getUpdateCount()));
        }
    }

    private void deleteEvents(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_EVENTS)) {
            statement.execute();
            LOG.info("deleteAllEvents", String.format("Delete events result: %d", statement.getUpdateCount()));
        }
    }

    private void deletePipeState(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_PIPE_STATE)) {
            statement.execute();
            LOG.info("deletePipeState", String.format("Delete pipe state result: %d", statement.getUpdateCount()));
        }
    }

    private void vacuumDatabase(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.VACUUM_DB)) {
            statement.execute();
            LOG.info("vacuumDatabase", String.format("Vacuum result: %d", statement.getUpdateCount()));
        }
    }

    private void shrinkMemory(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.SHRINK_MEMORY)) {
            statement.execute();
            LOG.info("shrinkMemory","Memory shrunk");
        }
    }

    private void checkpointWalFile(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CHECKPOINT_DB)) {
            statement.execute();
            LOG.info("checkPointDatabase", "checkpointed database");
        }
    }

    public void compactUpTo(final ZonedDateTime zonedDateTime) {
        // We may want a interface - Compactable - for this method signature
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(SQLiteQueries.COMPACT)) {
            Timestamp threshold = Timestamp.valueOf(zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
            statement.setTimestamp(1, threshold);
            statement.setTimestamp(2, threshold);
            final int rowsAffected = statement.executeUpdate();
            LOG.info("compaction", "compacted " + rowsAffected + " rows");
            shrinkMemory(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setStatementParametersForInsertMessageQuery(
            final PreparedStatement statement, final Message message) throws SQLException {
        try {
            statement.setLong(1, message.getOffset());
            statement.setString(2, message.getKey());
            statement.setString(3, message.getContentType());
            statement.setString(4, message.getType());
            statement.setTimestamp(5, Timestamp.valueOf(message.getCreated().withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime()));
            statement.setString(6, message.getData());
            statement.setInt(7, JsonHelper.toJson(message).length());
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}