package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CDCDocumentId {
    @JsonProperty("$oid")
    public String oid;

    @Override
    public String toString() {
        return oid;
    }
}
