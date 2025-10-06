package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DTOAccount {
    @JsonProperty("_id")
    public String _id;

    @JsonProperty("user_id")
    public JsonNode user_id;
}
