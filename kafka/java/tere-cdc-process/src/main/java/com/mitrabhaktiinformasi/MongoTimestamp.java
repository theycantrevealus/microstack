package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoTimestamp {
    @JsonProperty("t")
    public long t;

    @JsonProperty("i")
    public int i;
}
