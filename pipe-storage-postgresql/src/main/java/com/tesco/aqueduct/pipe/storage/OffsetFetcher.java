package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Value;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
public class OffsetFetcher {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));
    private final int readDelaySeconds;

    private String currentTimestamp = "CURRENT_TIMESTAMP";

    public OffsetFetcher(
        @Value("${persistence.read.read-delay-seconds}") final int readDelaySeconds
    ) {
        this.readDelaySeconds = readDelaySeconds;
    }

    @Cacheable(value="latest-offset-cache", parameters = "")
    public long getGlobalLatestOffset(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();

        try (PreparedStatement statement = getLatestOffsetStatement(connection);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong("last_offset");
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getLatestOffsetWithConnection:time", Long.toString(end - start));
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
}
