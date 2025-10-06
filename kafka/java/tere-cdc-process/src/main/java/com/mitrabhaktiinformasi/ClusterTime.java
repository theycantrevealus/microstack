package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterTime {
    @JsonProperty("$timestamp")
    public MongoTimestamp timestamp;
}
