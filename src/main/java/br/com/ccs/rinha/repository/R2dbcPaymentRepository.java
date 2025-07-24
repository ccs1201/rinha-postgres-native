package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

@Repository
public class R2dbcPaymentRepository {

    private static final Logger log = LoggerFactory.getLogger(R2dbcPaymentRepository.class);

    private static final String SQL_INSERT = """
        INSERT INTO payments (correlation_id, amount, requested_at, is_default)
        VALUES ($1, $2, $3, $4)
    """;

    private static final String SQL_SUMMARY = """
        SELECT 
            SUM(CASE WHEN is_default THEN 1 ELSE 0 END) as default_count,
            SUM(CASE WHEN is_default THEN amount ELSE 0 END) as default_amount,
            SUM(CASE WHEN NOT is_default THEN 1 ELSE 0 END) as fallback_count,
            SUM(CASE WHEN NOT is_default THEN amount ELSE 0 END) as fallback_amount
        FROM payments
        WHERE requested_at BETWEEN $1 AND $2
    """;

    private final DatabaseClient client;

    public R2dbcPaymentRepository(DatabaseClient client) {
        this.client = client;
    }

    public Mono<Void> save(PaymentRequest request) {
        return client.sql(SQL_INSERT)
                .bind("$1", request.correlationId)
                .bind("$2", request.amount)
                .bind("$3", request.requestedAt)
                .bind("$4", request.isDefault)
                .then() // Mono<Void>
                .doOnSuccess(v -> log.info("Saved payment: {}", request))
                .doOnError(e -> log.error("Failed to save payment", e));
    }

    public Mono<PaymentSummary> getSummary(OffsetDateTime from, OffsetDateTime to) {
        if (from == null) from = OffsetDateTime.now().minusMinutes(5);
        if (to == null) to = OffsetDateTime.now();

        return client.sql(SQL_SUMMARY)
                .bind("$1", from)
                .bind("$2", to)
                .map((row, meta) -> new PaymentSummary(
                        new PaymentSummary.Summary(
                                row.get("default_count", Long.class) != null ? row.get("default_count", Long.class) : 0L,
                                Optional.ofNullable(row.get("default_amount", BigDecimal.class)).orElse(BigDecimal.ZERO)
                        ),
                        new PaymentSummary.Summary(
                                row.get("fallback_count", Long.class) != null ? row.get("fallback_count", Long.class) : 0L,
                                Optional.ofNullable(row.get("fallback_amount", BigDecimal.class)).orElse(BigDecimal.ZERO)
                        )
                ))
                .one()
                .doOnError(e -> log.error("Failed to get summary", e));
    }

    public Mono<Void> purge() {
        return client.sql("DELETE FROM payments")
                .then()
                .doOnSuccess(v -> log.info("Purged payments table"))
                .doOnError(e -> log.error("Failed to purge payments", e));
    }
}
