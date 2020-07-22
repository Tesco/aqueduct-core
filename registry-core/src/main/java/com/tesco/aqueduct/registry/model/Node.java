package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Builder;
import lombok.Data;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;

@Builder(toBuilder = true)
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Node {
    /**
     * Name of the group, usually store number 
     */
    private final String group;

    /**
     * Self URL resolvable by peers in the same group
     */
    private final URL localUrl;

    /**
     * The offset as last reported by this node
     */
    @JsonSerialize(using = ToStringSerializer.class)
    private final long offset;

    /**
     * Status as last reported by this node (computed status might be different)
     */
    private final Status status;

    /**
     * List of nodes in order that current node will try to connect to
     */
    private final List<URL> following;

    /**
     * List of nodes in order that current was last told to follow.
     * Populated on server side.
     */
    private final List<URL> requestedToFollow;

    /**
     * Populated on server side as NOW
     */
    private final ZonedDateTime lastSeen;

    /**
     * Fields populated by pipe
     */
    private final Map<String, String> pipe;

    /**
     * Fields representing offsets
     */
    private final Map<String, String> offsets;

    /**
     * Fields populated by provider
     */
    private final Map<String, String> provider;

    public String getId() {
        if (group == null) {
            return localUrl.toString();
        } else {
            return String.format("%s|%s", group, localUrl.toString());
        }
    }

    @JsonIgnore
    public String getHost() {
        return localUrl.getHost();
    }

    public Node buildWith(List<URL> followUrls) {
        return this.toBuilder()
            .requestedToFollow(followUrls)
            .lastSeen(ZonedDateTime.now())
            .build();
    }

    @JsonIgnore
    public boolean isOffline() {
        return getStatus() == OFFLINE;
    }

    // All nodes for a location will be in the same subgroup
    @JsonIgnore
    public String getSubGroupId() {
        return "subGroupId";
    }

    @JsonIgnore
    public String getPipeVersion() {
        return pipe.get("v");
    }

    @JsonIgnore
    public boolean isSubGroupIdDifferent(Node node) {
        return !node.getSubGroupId().equals(getSubGroupId());
    }
}
