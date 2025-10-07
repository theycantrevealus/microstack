package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.databind.JsonNode;

public class DTORedeemEnriched {
    public boolean isEligible = false;
    public String reason = "";
    public JsonNode keyword;
    public JsonNode program;
    public JsonNode customer;

    public DTORedeemEnriched() {
    }

    public boolean setEligibility(boolean isEligible, String reason) {
        this.isEligible = isEligible;
        this.reason = reason;
        return this.isEligible;
    }

    public DTORedeemEnriched(JsonNode keyword, JsonNode program, JsonNode customer) {
        this.keyword = keyword;
        this.program = program;
        this.customer = customer;
    }
}