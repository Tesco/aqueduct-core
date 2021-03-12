package Helper

import com.tesco.aqueduct.pipe.api.Message
import groovy.sql.Sql

import javax.sql.DataSource
import java.sql.Timestamp

class SqlWrapper {

    private Sql sql

    SqlWrapper(DataSource dataSource) {
        this.sql = setupPostgres(dataSource)
    }

    Sql setupPostgres(DataSource dataSource) {
        sql = new Sql(dataSource.connection)
        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS CLUSTERS;
        DROP TABLE IF EXISTS CLUSTER_CACHE;
        DROP TABLE IF EXISTS REGISTRY;
        DROP TABLE IF EXISTS NODE_REQUESTS;
          
        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1,
            time_to_live TIMESTAMP NULL
        );
        
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        CREATE TABLE CLUSTER_CACHE(
            location_uuid VARCHAR PRIMARY KEY NOT NULL,
            cluster_ids BIGINT[] NOT NULL,
            expiry TIMESTAMP NOT NULL,
            valid BOOLEAN NOT NULL DEFAULT TRUE
        );
        
        CREATE TABLE REGISTRY(
            group_id VARCHAR PRIMARY KEY NOT NULL,
            entry JSON NOT NULL,
            version integer NOT NULL
        );

        CREATE TABLE NODE_REQUESTS(
            host_id VARCHAR PRIMARY KEY NOT NULL,
            bootstrap_requested timestamp NOT NULL,
            bootstrap_type VARCHAR NOT NULL,
            bootstrap_received timestamp
        );

        INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');
        """)
        return sql
    }

    void insertWithCluster(Message msg, Long clusterId, def time = Timestamp.valueOf(msg.created.toLocalDateTime()), int maxMessageSize=0) {
        sql.execute(
                "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
                msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize, clusterId
        )
    }

    Long insertCluster(String clusterUuid){
        sql.executeInsert("INSERT INTO CLUSTERS(cluster_uuid) VALUES (?);", [clusterUuid]).first()[0] as Long
    }
}
