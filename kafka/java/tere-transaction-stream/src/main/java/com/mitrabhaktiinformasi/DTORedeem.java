package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DTORedeem {
    @JsonProperty("data")
    public DTOData data;

    @JsonProperty("account")
    public DTOAccount account;

    @JsonProperty("token")
    public String token;

    @JsonProperty("transaction_id")
    public String transaction_id;

    @JsonProperty("path")
    public String path;

    @JsonProperty("keyword_priority")
    public String keyword_priority;

    public long eventTime;

    public void resolveEventTime() {
        this.eventTime = System.currentTimeMillis();
    }
}
