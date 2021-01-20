package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.LocationService;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import lombok.Data;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ClusterStorage implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(ClusterStorage.class));

    private static final String CLUSTER_CACHE_QUERY = "SELECT location_uuid, cluster_ids, expiry, valid FROM cluster_cache WHERE " +
        "location_uuid = ?";

    private static final String INSERT_CLUSTER = " INSERT INTO CLUSTERS (cluster_uuid) VALUES (?) ON CONFLICT DO NOTHING;";

    private static final String UPSERT_CLUSTER_CACHE = " INSERT INTO CLUSTER_CACHE (location_uuid, cluster_ids, expiry) VALUES (?, ?, ?) " +
        "ON CONFLICT (location_uuid) DO UPDATE SET cluster_ids = ?, expiry = ?, valid = true;";

    private static final String UPDATE_CLUSTER_CACHE = " UPDATE CLUSTER_CACHE SET cluster_ids=?,expiry=? where location_uuid = ? and valid = true";

    private static final String SELECT_CLUSTER_ID = " SELECT cluster_id FROM CLUSTERS WHERE ((cluster_uuid)::text = ANY (string_to_array(?, ',')));";

    private final DataSource dataSource;
    private final LocationService locationService;
    private final Duration cacheExpiryDuration;
    private static final String CLUSTER_IDS_TYPE = "BIGINT";

    public ClusterStorage(DataSource dataSource, LocationService locationService, Duration cacheExpiryDuration) {
        this.dataSource = dataSource;
        this.locationService = locationService;
        this.cacheExpiryDuration = cacheExpiryDuration;
    }

    @Override
    public List<Long> getClusterIds(String locationUuid) {
        Optional<ClusterCache> clusterCache = getClusterIdsFromCache(locationUuid);

        return clusterCache
            .filter(this::isCached)
            .map(ClusterCache::getClusterIds)
            .orElseGet(() ->
                resolveClusterIds(locationUuid, clusterCache).orElseGet(() -> getClusterIds(locationUuid))
            );
    }

    private boolean isCached(ClusterCache entry) {
        return entry.isValid() && entry.getExpiry().isAfter(LocalDateTime.now());
    }

    private Optional<List<Long>> resolveClusterIds(String locationUuid, Optional<ClusterCache> clusterCache) {
        long start = System.currentTimeMillis();

        final List<String> resolvedClusterUuids = locationService.getClusterUuids(locationUuid);

        try (Connection newConnection = dataSource.getConnection()) {
            final List<Long> clusterIds = resolveClusterIdsFor(resolvedClusterUuids, newConnection);

            if (cacheNotPresentOrInvalid(clusterCache)) {
                upsertClusterCache(locationUuid, clusterIds, newConnection);
                return Optional.of(clusterIds);

            } else {
                // the entry is present and valid but it wasn't a cache hit, hence we are here only when it is expired
                final int updatedRowCount = updateClusterCache(locationUuid, clusterIds, newConnection);

                if (updatedRowCount == 0) {
                    // the entry has been invalidated while the request to location service was in flight
                    return Optional.empty();
                } else {
                    return Optional.of(clusterIds);
                }
            }
        } catch (SQLException exception) {
            LOG.error("cluster storage", "resolve cluster ids", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runResolveClusterIds:time", Long.toString(end - start));
        }
    }

    private boolean cacheNotPresentOrInvalid(Optional<ClusterCache> clusterCache) {
        return !clusterCache.isPresent() || !clusterCache.get().isValid;
    }

    private List<Long> resolveClusterIdsFor(List<String> resolvedClusterUuids, Connection newConnection) {
        insertClusterUuids(resolvedClusterUuids, newConnection);
        return fetchClusterIdsFor(resolvedClusterUuids, newConnection);
    }

    private Optional<ClusterCache> getClusterIdsFromCache(String locationUuid) {
        long start = System.currentTimeMillis();
        try (Connection connection = dataSource.getConnection()) {
            return resolveLocationUuidToClusterIds(locationUuid, connection);
        } catch (SQLException exception) {
            LOG.error("cluster storage", "get cluster ids", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runGetClusterIdsFromCache:time", Long.toString(end - start));
        }
    }

    private Optional<ClusterCache> resolveLocationUuidToClusterIds(String locationUuid, Connection connection) {
        try(PreparedStatement statement = getLocationToClusterIdsStatement(connection, locationUuid)) {
            return runLocationToClusterIdsQuery(statement);
        } catch (SQLException exception) {
            LOG.error("cluster storage", "resolve location to clusterIds", exception);
            throw new RuntimeException(exception);
        }
    }

    private Optional<ClusterCache> runLocationToClusterIdsQuery(final PreparedStatement query) throws SQLException {
        long start = System.currentTimeMillis();
        try (ResultSet rs = query.executeQuery()) {
            if (rs.next()) {
                final String locationUuid = rs.getString("location_uuid");

                Array clusterIdArray = rs.getArray("cluster_ids");
                Long[] array = (Long[]) clusterIdArray.getArray();
                final List<Long> clusterIds = Arrays.asList(array);

                final LocalDateTime expiry = rs.getTimestamp("expiry").toLocalDateTime();
                final boolean isValid = rs.getBoolean("valid");

                return Optional.of(new ClusterCache(locationUuid, clusterIds, expiry, isValid));
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

    private void upsertClusterCache(String locationUuid, List<Long> clusterids, Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_CLUSTER_CACHE)) {
            Array clustersPgArray = connection.createArrayOf(CLUSTER_IDS_TYPE, clusterids.toArray());
            Timestamp expiry = Timestamp.valueOf(LocalDateTime.now().plus(cacheExpiryDuration));

            statement.setString(1, locationUuid);
            statement.setArray(2, clustersPgArray);
            statement.setTimestamp(3, expiry);
            statement.setArray(4, clustersPgArray);
            statement.setTimestamp(5, expiry);

            statement.execute();
        } catch (SQLException exception) {
            LOG.error("cluster storage", "upsert cluster cache statement", exception);
            throw new RuntimeException(exception);
        }
        LOG.info("cluster storage", "New cluster cache upserted for: " + locationUuid);
    }

    private int updateClusterCache(String locationUuid, List<Long> clusterids, Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(UPDATE_CLUSTER_CACHE)) {
            Array clustersPgArray = connection.createArrayOf(CLUSTER_IDS_TYPE, clusterids.toArray());
            Timestamp expiry = Timestamp.valueOf(LocalDateTime.now().plus(cacheExpiryDuration));

            statement.setArray(1, clustersPgArray);
            statement.setTimestamp(2, expiry);
            statement.setString(3, locationUuid);

            int updatedRowsCount = statement.executeUpdate();
            LOG.info("cluster storage", "cluster cache updated for: " + locationUuid + ", rows updated: " + updatedRowsCount);
            return updatedRowsCount;
        } catch (SQLException exception) {
            LOG.error("cluster storage", "insert cluster cache statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private List<Long> fetchClusterIdsFor(List<String> clusterUuids, Connection connection) {
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

    @Data
    private static class ClusterCache {
        private final String locationUuid;
        private final List<Long> clusterIds;
        private final LocalDateTime expiry;
        private final boolean isValid;
    }
}
