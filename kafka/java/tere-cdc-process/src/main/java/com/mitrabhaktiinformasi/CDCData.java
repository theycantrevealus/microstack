package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CDCData {
    @JsonProperty("operationType")
    public String operationType;

    @JsonProperty("fullDocument")
    public JsonNode fullDocument;

    @JsonProperty("updateDescription")
    public CDCUpdateDescription updateDescription;

    @JsonProperty("documentKey")
    public CDCDocumentKey documentKey;

    @JsonProperty("wallTime")
    public WallTime wallTime;

    @JsonProperty("clusterTime")
    public ClusterTime clusterTime;

    public long eventTime;

    public void resolveEventTime() {
        if (wallTime != null && wallTime.date != null) {
            this.eventTime = wallTime.date;
        } else if (clusterTime != null && clusterTime.timestamp != null) {
            this.eventTime = clusterTime.timestamp.t * 1000L;
        } else {
            this.eventTime = System.currentTimeMillis();
        }
    }
}
