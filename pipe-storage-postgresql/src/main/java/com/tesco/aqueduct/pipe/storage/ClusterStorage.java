package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.LocationService;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.time.temporal.ChronoUnit.MINUTES;

public class ClusterStorage implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(ClusterStorage.class));
    private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    private static final String CLUSTER_CACHE_QUERY = "SELECT cluster_ids FROM cluster_cache WHERE location_uuid = ? AND expiry > " + CURRENT_TIMESTAMP + ";";
    private static final String INSERT_CLUSTER = " INSERT INTO CLUSTERS (cluster_uuid) VALUES (?) ON CONFLICT DO NOTHING;";
    private static final String INSERT_CLUSTER_CACHE = " INSERT INTO CLUSTER_CACHE (location_uuid, cluster_ids, expiry) VALUES (?, ?, ?) ON CONFLICT DO NOTHING;";
    private static final String SELECT_CLUSTER_ID = " SELECT cluster_id FROM CLUSTERS WHERE ((cluster_uuid)::text = ANY (string_to_array(?, ',')));";


    private final DataSource dataSource;
    private final LocationService locationService;
    private static final String CLUSTER_IDS_TYPE = "BIGINT";

    public ClusterStorage(DataSource dataSource, LocationService locationService) {
        this.dataSource = dataSource;
        this.locationService = locationService;
    }

    @Override
    public Optional<List<Long>> getClusterIds(String locationUuid) {
        Optional<List<Long>> clusterIdsFromCache = getClusterIdsFromCache(locationUuid);

        if(clusterIdsFromCache.isPresent()) {
            return clusterIdsFromCache;
        } else {
            return resolveClusterIds(locationUuid, clusterIdsFromCache);
        }
    }

    private Optional<List<Long>> resolveClusterIds(String locationUuid, Optional<List<Long>> clusterIdsFromCache) {
        if (clusterIdsFromCache.isPresent()) {
            return clusterIdsFromCache;
        } else {
            final List<String> resolvedClusterUuids = locationService.getClusterUuids(locationUuid);

            try (Connection newConnection = dataSource.getConnection()) {
                insertClusterUuids(resolvedClusterUuids, newConnection);
                final List<Long> newClusterIds = resolveClusterUuidsToClusterIds(resolvedClusterUuids, newConnection);
                insertClusterCache(locationUuid, newClusterIds, newConnection);
                return Optional.of(newClusterIds);
            } catch (SQLException exception) {
                LOG.error("cluster storage", "resolve cluster ids", exception);
                throw new RuntimeException();
            }
        }
    }

    private Optional<List<Long>> getClusterIdsFromCache(String locationUuid) {
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection()) {
            return resolveLocationUuidToClusterIds(locationUuid, connection);
        } catch (SQLException exception) {
            LOG.error("cluster storage", "get cluster ids", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runGetClusterIds:time", Long.toString(end - start));
        }
    }

    private Optional<List<Long>> resolveLocationUuidToClusterIds(String locationUuid, Connection connection) {
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

    private void insertClusterUuids(List<String> clusterUuids, Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_CLUSTER)) {
            for (String clusterUuid : clusterUuids) {
                statement.setString(1, clusterUuid);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException exception) {
            LOG.error("cluster storage", "insert clusters statement", exception);
            throw new RuntimeException(exception);
        }
        LOG.info("cluster storage", "New clusters inserted: " + clusterUuids);
    }

    private void insertClusterCache(String locationUuid, List<Long> clusterids, Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_CLUSTER_CACHE)) {
            statement.setString(1, locationUuid);
            statement.setArray(2, connection.createArrayOf(CLUSTER_IDS_TYPE, clusterids.toArray()));
            statement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now().plus(10, MINUTES)));

            statement.execute();
        } catch (SQLException exception) {
            LOG.error("cluster storage", "insert cluster cache statement", exception);
            throw new RuntimeException(exception);
        }
        LOG.info("cluster storage", "New cluster cache inserted for: " + locationUuid);
    }

    private List<Long> resolveClusterUuidsToClusterIds(List<String> clusterUuids, Connection connection) {
        final List<Long> clusterIds = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(SELECT_CLUSTER_ID)) {

            final String strClusters = String.join(",", clusterUuids);
            statement.setString(1, strClusters);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    LOG.debug("location cluster validator", "matched clusterId");
                    final long cluster_id = resultSet.getLong("cluster_id");
                    clusterIds.add(cluster_id);
                }
            }
        } catch (SQLException sqlException) {
            LOG.error("cluster storage", "resolve cluster uuids to cluster ids statement", sqlException);
            throw new RuntimeException(sqlException);
        }
        return clusterIds;
    }
}
