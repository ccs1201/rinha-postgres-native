package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import static java.util.Objects.isNull;

@Repository
public class JdbcPaymentRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcPaymentRepository.class);
    private static final String SQL_INSERT = "INSERT INTO payments (correlation_id, amount, requested_at, is_default) VALUES (?, ?, ?, ?)";
    private static final String SQL_SUMMARY = """
            SELECT 
                SUM(CASE WHEN is_default = true THEN 1 ELSE 0 END) as default_count,
                SUM(CASE WHEN is_default = true THEN amount ELSE 0 END) as default_amount,
                SUM(CASE WHEN is_default = false THEN 1 ELSE 0 END) as fallback_count,
                SUM(CASE WHEN is_default = false THEN amount ELSE 0 END) as fallback_amount
            FROM payments 
            WHERE requested_at >= ? AND requested_at <= ?
            """;
    private final DataSource dataSource;

    public JdbcPaymentRepository(DataSource dataSource,
                                 @Value("${spring.datasource.hikari.maximum-pool-size}") int poolSize,
                                 @Value("${spring.datasource.hikari.minimum-idle}") int minIdle) {
        this.dataSource = dataSource;
        log.info("JDBC Pool size: {}", poolSize);
        log.info("JDBC Min Idle: {}", minIdle);
    }

    public void save(PaymentRequest request) {

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_INSERT)) {
            stmt.setObject(1, request.correlationId);
            stmt.setBigDecimal(2, request.amount);
            stmt.setObject(3, request.requestedAt);
            stmt.setBoolean(4, request.isDefault);
            stmt.executeUpdate();

        } catch (SQLException e) {
            log.error("Save error {}", e.getMessage(), e);
        }
    }

    public PaymentSummary getSummary(OffsetDateTime from, OffsetDateTime to) {

        if (isNull(from)) {
            from = OffsetDateTime.now().minusMinutes(5);
        }

        if (isNull(to)) {
            to = OffsetDateTime.now();
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_SUMMARY)) {

            stmt.setObject(1, from);
            stmt.setObject(2, to);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new PaymentSummary(
                            new PaymentSummary.Summary(rs.getLong("default_count"),
                                    rs.getBigDecimal("default_amount") != null ? rs.getBigDecimal("default_amount") : BigDecimal.ZERO),
                            new PaymentSummary.Summary(rs.getLong("fallback_count"),
                                    rs.getBigDecimal("fallback_amount") != null ? rs.getBigDecimal("fallback_amount") : BigDecimal.ZERO)
                    );
                }
                return new PaymentSummary(new PaymentSummary.Summary(0, BigDecimal.ZERO), new PaymentSummary.Summary(0, BigDecimal.ZERO));
            }

        } catch (SQLException e) {
            log.error("Get summary error: {}", e.getMessage(), e);
            return new PaymentSummary(new PaymentSummary.Summary(0, BigDecimal.ZERO), new PaymentSummary.Summary(0, BigDecimal.ZERO));
        }
    }

    public void purge() {
        String sql = "DELETE FROM payments";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}