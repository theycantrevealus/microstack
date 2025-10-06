package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.databind.JsonNode;

public class DTORedeemEnriched {
    public JsonNode keyword;
    public JsonNode program;
    public JsonNode customer;

    public DTORedeemEnriched() {
    }

    public DTORedeemEnriched(JsonNode keyword, JsonNode program, JsonNode customer) {
        this.keyword = keyword;
        this.program = program;
        this.customer = customer;
    }
}