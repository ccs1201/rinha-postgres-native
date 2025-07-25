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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    LinkedBlockingQueue<PaymentRequest> queue = new LinkedBlockingQueue<>(5000);


    public JdbcPaymentRepository(DataSource dataSource,
                                 @Value("${spring.datasource.hikari.maximum-pool-size}") int poolSize,
                                 @Value("${spring.datasource.hikari.minimum-idle}") int minIdle) {
        this.dataSource = dataSource;
        log.info("JDBC Pool size: {}", poolSize);
        log.info("JDBC Min Idle: {}", minIdle);

        for (int i = 0; i < poolSize - 1; i++) {
            startWorker(i);
        }
    }

    private void startWorker(int workerIndex) {
        log.info("Starting repository-worker-{}", workerIndex);
        Thread.ofVirtual().name("repository-worker-" + workerIndex).start(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(SQL_INSERT)) {

                conn.setAutoCommit(false);

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (queue.size() >= 10) {
                            long now = Instant.now().toEpochMilli();
                            List<PaymentRequest> batch = new ArrayList<>(100);
                            queue.drainTo(batch, 100);

                            if (batch.isEmpty()) continue;

                            for (PaymentRequest pr : batch) {
                                stmt.setObject(1, pr.correlationId);
                                stmt.setBigDecimal(2, pr.amount);
                                stmt.setObject(3, pr.requestedAt);
                                stmt.setBoolean(4, pr.isDefault);
                                stmt.addBatch();
                            }

                            stmt.executeBatch();
                            conn.commit();

                            long elapsed = Instant.now().toEpochMilli() - now;
                            log.info("BATCH Size {} Processed in {}ms Queue size {})", batch.size(), elapsed, queue.size());

                            continue;
                        }

                        // fallback para unitário
                        PaymentRequest pr = queue.take();
                        stmt.setObject(1, pr.correlationId);
                        stmt.setBigDecimal(2, pr.amount);
                        stmt.setObject(3, pr.requestedAt);
                        stmt.setBoolean(4, pr.isDefault);
                        stmt.executeUpdate();
                        conn.commit();

                        long elapsed = Instant.now().toEpochMilli() - pr.receivedAt.toInstant().toEpochMilli();
                        log.info("Payment processed in {}ms queue size {}", elapsed, queue.size());

                    } catch (Exception e) {
                        log.error("Error inserting payment", e);
                    }
                }
            } catch (Exception e) {
                log.error("Worker failure", e);
            }
        });
        log.info("repository-worker-{} started", workerIndex);
    }


    public void saveAsync(PaymentRequest paymentRequest) {
        queue.offer(paymentRequest);

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
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    public void addToBatchInsert(PaymentRequest paymentRequest) {
//        batchInsert.add(paymentRequest);
//    }

    private class BatchInsert {
        private final Connection connection;
        private PreparedStatement preparedStatement;
        private final AtomicInteger batchInsertCounter = new AtomicInteger(0);
        private final Object lock = new Object();

        public BatchInsert() throws SQLException {
            this.connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            this.preparedStatement = connection.prepareStatement(SQL_INSERT);
        }

        public void add(PaymentRequest paymentRequest) {
            try {
                addToBatch(paymentRequest);
                if (batchInsertCounter.incrementAndGet() >= 100) {
                    log.info("Batch insert result {}", flushBatch());
                    batchInsertCounter.set(0);
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        private void addToBatch(PaymentRequest paymentRequest) throws SQLException {
            synchronized (lock) {
                preparedStatement.setObject(1, paymentRequest.correlationId);
                preparedStatement.setBigDecimal(2, paymentRequest.amount);
                preparedStatement.setObject(3, paymentRequest.requestedAt);
                preparedStatement.setBoolean(4, paymentRequest.isDefault);
                preparedStatement.addBatch();
            }
        }

        public int flushBatch() {
            try {
                synchronized (lock) {
                    var result = preparedStatement.executeBatch().length;
                    connection.commit();
                    close();
                    return result;
                }
            } catch (SQLException e) {
                log.error("", e);
                return 0;
            }
        }

        public void close() {
            try {
                preparedStatement.close();
                preparedStatement = connection.prepareStatement(SQL_INSERT);
            } catch (Exception e) {
                log.error("Batch insert close error {}", e.getMessage(), e);
            }
        }
    }
}