package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Till;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;

public class PostgreSQLTillStorage implements TillStorage {

    private final DataSource dataSource;
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLTillStorage.class));
    private static final String QUERY_INSERT_TILL =
        "INSERT INTO tills (host_id, bootstrap_requested, bootstrap_type)" +
            "VALUES (" +
            "?, " +
            "?, " +
            "? " +
            ")" +
        "ON CONFLICT (host_id) DO UPDATE SET " +
            "host_id = EXCLUDED.host_id, " +
            "bootstrap_requested = EXCLUDED.bootstrap_requested, " +
            "bootstrap_type = EXCLUDED.bootstrap_type, " +
            "bootstrap_received = null;";

    public PostgreSQLTillStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateTill(Till till) {
         try (Connection connection = getConnection()) {
             insert(connection, till);
         } catch (SQLException exception) {
             LOG.error("updateTill", "insert a till", exception);
             throw new RuntimeException(exception);
         }
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        try {
            return dataSource.getConnection();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getConnection:time", Long.toString(end - start));
        }
    }

    private void insert(
        final Connection connection,
        final Till till
    ) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(till.getBootstrap().getRequestedDate().atOffset(ZoneOffset.UTC).toLocalDateTime());

        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_TILL)) {
            statement.setString(1, till.getHostId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, till.getBootstrap().getType().toString());
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("hostId insert:time", Long.toString(end - start));
        }
    }

}
