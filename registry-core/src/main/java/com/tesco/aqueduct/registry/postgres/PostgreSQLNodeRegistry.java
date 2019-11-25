package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.utils.RegistryLogger;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.registry.model.StateSummary;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class PostgreSQLNodeRegistry implements NodeRegistry {
    private static final long OPTIMISTIC_LOCKING_COOLDOWN_MS = 500L;
    private static final int OPTIMISTIC_LOCKING_COOLDOWN_RANDOM_BOUND = 500;
    private static final Random random = new Random();
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private final PostgresNodeGroupStorage nodeGroupStorage;

    public PostgreSQLNodeRegistry(final DataSource dataSource, final URL cloudUrl, final Duration offlineDelta) {
        this(dataSource, cloudUrl, offlineDelta, new PostgresNodeGroupStorage());
    }

    PostgreSQLNodeRegistry(final DataSource dataSource, final URL cloudUrl, final Duration offlineDelta, PostgresNodeGroupStorage nodeGroupStorage) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
        this.nodeGroupStorage = nodeGroupStorage;
    }

    @Override
    public List<URL> register(final Node nodeToRegister) {
        int count = 0;
        while (count < 10) {
            try (Connection connection = getConnection()) {
                final PostgresNodeGroup group = nodeGroupStorage.getNodeGroup(connection, nodeToRegister.getGroup());
                Node node = group.getById(nodeToRegister.getId());
                if (node != null) {
                    node = updateNodeToFollow(nodeToRegister, node.getRequestedToFollow());
                    group.updateNode(node);
                } else {
                    node = group.add(nodeToRegister, cloudUrl);
                }
                group.persist(connection);
                return node.getRequestedToFollow();
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "register node", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    LOG.info("Postgresql version changed exception", Integer.toString(++count));
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN_MS + random.nextInt(OPTIMISTIC_LOCKING_COOLDOWN_RANDOM_BOUND));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Failed to register");
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

    @Override
    public StateSummary getSummary(final long offset, final String status, final List<String> groupIds) {
        List<PostgresNodeGroup> groups = getPostgresNodeGroups(groupIds);

        final ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);
        groups.forEach(group -> group.markNodesOfflineIfNotSeenSince(threshold));

        final List<Node> followers = groups.stream()
            .map(group -> group.nodes)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        return new StateSummary(getCloudNode(offset, status), followers);
    }

    private List<PostgresNodeGroup> getPostgresNodeGroups(List<String> groupIds) {
        List<PostgresNodeGroup> groups;
        try (Connection connection = getConnection()) {
            groups = nodeGroupStorage.getNodeGroups(connection, groupIds);
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
        return groups;
    }

    private Node getCloudNode(final long offset, final String status) {
        return Node.builder()
            .localUrl(cloudUrl)
            .offset(offset)
            .status(status)
            .following(Collections.emptyList())
            .lastSeen(ZonedDateTime.now())
            .build();
    }

    @Override
    public boolean deleteNode(final String groupId, final String host) {
        while (true) {
            try (Connection connection = getConnection()) {
                final PostgresNodeGroup nodeGroup = nodeGroupStorage.getNodeGroup(connection, groupId);

                if(nodeGroup.isEmpty()) {
                    return false;
                } else {
                    return deleteExistingNode(connection, host, nodeGroup);
                }
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "deleteNode", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN_MS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean deleteExistingNode(final Connection connection, final String host, final PostgresNodeGroup group) throws IOException, SQLException {
        final boolean foundNode = group.removeByHost(host);
        if (foundNode) {
            if (group.isEmpty()) {
                group.delete(connection);
            } else {
                group.rebalance(cloudUrl);
                group.persist(connection);
            }
            return true;
        }
        return false;
    }

    private Node updateNodeToFollow(final Node node, final List<URL> followURLs) {
        return node.toBuilder()
            .requestedToFollow(followURLs)
            .lastSeen(ZonedDateTime.now())
            .build();
    }
}
