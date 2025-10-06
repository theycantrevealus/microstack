package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DTOData {
    @JsonProperty("locale")
    public String locale;

    @JsonProperty("msisdn")
    public String msisdn;

    @JsonProperty("keyword")
    public String keyword;

    @JsonProperty("transaction_id")
    public String transaction_id;

    @JsonProperty("channel_id")
    public String channel_id;

    @JsonProperty("callback_url")
    public String callback_url;

    @JsonProperty("transaction_source")
    public String transaction_source;

    @JsonProperty("send_notification")
    public Boolean send_notification;
}
