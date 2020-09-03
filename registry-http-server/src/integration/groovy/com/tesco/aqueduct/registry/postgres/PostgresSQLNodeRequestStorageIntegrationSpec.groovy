package com.tesco.aqueduct.registry.postgres

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.registry.model.Bootstrap
import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.NodeRequest
import com.tesco.aqueduct.registry.model.NodeRequestStorage
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset

class PostgresSQLNodeRequestStorageIntegrationSpec extends Specification {

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    DataSource dataSource
    NodeRequestStorage nodeRequestStorage

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
            DROP TABLE IF EXISTS node_requests;
            
            CREATE TABLE node_requests(
                host_id VARCHAR PRIMARY KEY NOT NULL,
                bootstrap_requested timestamp NOT NULL,
                bootstrap_type VARCHAR NOT NULL,
                bootstrap_received timestamp
            );
        """)

        nodeRequestStorage = new PostgreSQLNodeRequestStorage(dataSource)
    }

    def "When a node request is updated an entry is added to the database"() {
        given: "a postgres node request storage"

        when: "bootstrap is requested"
        LocalDateTime now = LocalDateTime.now()
        nodeRequestStorage.save(new NodeRequest("host-id", new Bootstrap(BootstrapType.PROVIDER, now)))

        then: "data store contains the correct entry"
        def rows = sql.rows("SELECT * FROM node_requests;")
        Timestamp timestamp = Timestamp.valueOf(now.atOffset(ZoneOffset.UTC).toLocalDateTime())

        rows.get(0).getProperty("host_id") == "host-id"
        rows.get(0).getProperty("bootstrap_requested") == timestamp
        rows.get(0).getProperty("bootstrap_type") == "PROVIDER"
        rows.get(0).getProperty("bootstrap_received") == null
    }

    def "when a node request is updated twice, the first request is overwritten"() {
        given: "a postgres node request storage"

        when: "bootstrap is requested for the first time"
        LocalDateTime firstTime = LocalDateTime.now()
        nodeRequestStorage.save(new NodeRequest("host-id", new Bootstrap(BootstrapType.PROVIDER, firstTime)))

        and: "bootstrap is requested for the second time with different params"
        LocalDateTime secondTime = LocalDateTime.now()
        nodeRequestStorage.save(new NodeRequest("host-id", new Bootstrap(BootstrapType.PIPE_AND_PROVIDER, secondTime)))

        then: "data store contains the correct entry"
        def rows = sql.rows("SELECT * FROM node_requests;")
        Timestamp timestamp = Timestamp.valueOf(secondTime.atOffset(ZoneOffset.UTC).toLocalDateTime())

        rows.get(0).getProperty("host_id") == "host-id"
        rows.get(0).getProperty("bootstrap_requested") == timestamp
        rows.get(0).getProperty("bootstrap_type") == "PIPE_AND_PROVIDER"
        rows.get(0).getProperty("bootstrap_received") == null
    }

    @Unroll
    def "when I read with host-id it should return the BootstrapType #bootstrapType"() {
        given: "data is stored within a postgres node request storage"
        LocalDateTime now = LocalDateTime.now()
        nodeRequestStorage.save(new NodeRequest("host-id", new Bootstrap(bootstrapType, now)))

        when: "read is called with the host-id"
        def returnedBootstrap = nodeRequestStorage.requiresBootstrap("host-id")

        then: "the BootstrapType is returned"
        returnedBootstrap == bootstrapType

        where:
        bootstrapType << BootstrapType.values()
    }

    def "bootstrap request is only sent once"() {
        given: "data is stored within a postgres node request storage"
        LocalDateTime now = LocalDateTime.now()
        nodeRequestStorage.save(new NodeRequest("host-id", new Bootstrap(BootstrapType.PROVIDER, now)))

        when: "read is called with the host-id"
        def firstReturnedBootstrap = nodeRequestStorage.requiresBootstrap("host-id")

        and: "read is called for a second time"
        def secondReturnedBootstrap = nodeRequestStorage.requiresBootstrap("host-id")

        then: "bootstrap is only returned once"
        firstReturnedBootstrap == BootstrapType.PROVIDER
        secondReturnedBootstrap == BootstrapType.NONE
    }

    def "bootstrap request is made when node request doesn't exist"() {
        given: "the node request doesn't exist"

        when: "read is called with the host-id"
        def response = nodeRequestStorage.requiresBootstrap("host-id")

        then: "bootstrap is not returned"
        response == BootstrapType.NONE
    }
}
