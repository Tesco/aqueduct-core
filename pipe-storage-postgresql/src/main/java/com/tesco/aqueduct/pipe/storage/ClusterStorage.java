package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.LocationService;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import jdk.internal.net.http.common.Log;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ClusterStorage implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(ClusterStorage.class));
    private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    private static final String CLUSTER_CACHE_QUERY = "SELECT cluster_ids FROM cluster_cache WHERE location_uuid = ? AND expiry > " + CURRENT_TIMESTAMP + ";";

    private final DataSource dataSource;
    private final LocationService locationService;

    public ClusterStorage(DataSource dataSource, LocationService locationService) {
        this.dataSource = dataSource;
        this.locationService = locationService;
    }

    @Override
    public Optional<List<Long>> getClusterIds(String locationUuid) {
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection()) {
           return resolveLocationUuidToClusterIds(connection, locationUuid);
        } catch (SQLException exception) {
            LOG.error("cluster storage", "get cluster ids", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("rungetClusterIds:time", Long.toString(end - start));
        }
    }

    private Optional<List<Long>> resolveLocationUuidToClusterIds(Connection connection, String locationUuid) {
        try(PreparedStatement statement = getLocationToClusterIdsStatement(connection, locationUuid)) {
            return runLocationToClusterIdsQuery(statement);
        } catch (SQLException exception) {
            LOG.error("cluster storage", "resolve location to clusterIds", exception);
            throw new RuntimeException(exception);
        }
    }

    private Optional<List<Long>> runLocationToClusterIdsQuery(final PreparedStatement query) throws SQLException {
        long start = System.currentTimeMillis();
        try (ResultSet rs = query.executeQuery()) {
            if (rs.next()) {
                Array clusterIds = rs.getArray(1);
                Long[] array = (Long[]) clusterIds.getArray();
                return Optional.of(Arrays.asList(array));
            }

            return Optional.empty();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runLocationToClusterIdsQuery:time", Long.toString(end - start));
        }
    }

    private PreparedStatement getLocationToClusterIdsStatement(final Connection connection, final String locationUuid) {
        try {
            PreparedStatement query = connection.prepareStatement(CLUSTER_CACHE_QUERY);
            query.setString(1, locationUuid);
            return query;
        } catch (SQLException exception) {
            LOG.error("cluster storage", "get location to clusterIds statement", exception);
            throw new RuntimeException(exception);
        }
    }
}
