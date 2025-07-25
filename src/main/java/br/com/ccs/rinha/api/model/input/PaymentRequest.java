package br.com.ccs.rinha.api.model.input;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public final class PaymentRequest {
    public UUID correlationId;
    public BigDecimal amount;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    public OffsetDateTime requestedAt;
    public boolean isDefault;
    public OffsetDateTime receivedAt;

    private String json;

    public void setDefaultFalse() {
        this.isDefault = false;
    }

    public void setDefaultTrue() {
        this.isDefault = true;
    }

    public String getJson() {
        if (json == null) {
            toJson();
        }
        return json;
    }

    private void toJson() {
        var sb = new StringBuilder(128);
        json = sb.append("{")
                .append("\"correlationId\":\"").append(correlationId).append("\",")
                .append("\"amount\":").append(amount).append(",")
                .append("\"requestedAt\":\"").append(requestedAt).append("\"")
                .append("}")
                .toString();
    }
}