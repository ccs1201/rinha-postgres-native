package br.com.ccs.rinha.api.controller;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import br.com.ccs.rinha.monitor.ExecutorMonitor;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import br.com.ccs.rinha.service.PaymentProcessorClientService;
import jakarta.annotation.PreDestroy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
public class PaymentController {

    private final PaymentProcessorClientService client;
    private final JdbcPaymentRepository repository;
    private final ExecutorService executor;
    private final ExecutorMonitor executorMonitor;

    public PaymentController(PaymentProcessorClientService client,
                             JdbcPaymentRepository repository,
                             ThreadPoolExecutor executor,
                             ExecutorMonitor executorMonitor) {
        this.client = client;
        this.repository = repository;
        this.executor = executor;
        this.executorMonitor = executorMonitor;
    }

    @PostMapping("/payments")
    public void createPayment(@RequestBody PaymentRequest paymentRequest) {
        executor.submit(() -> {
            paymentRequest.requestedAt = OffsetDateTime.now(ZoneOffset.UTC);
            client.processPayment(paymentRequest);
        }, executor);

    }

    @GetMapping("/payments-summary")
    public PaymentSummary getPaymentsSummary(@RequestParam(required = false) OffsetDateTime from,
                                             @RequestParam(required = false) OffsetDateTime to) {

        return repository.getSummary(from, to);
    }

    @PostMapping("/purge-payments")
    public ResponseEntity<Void> purgePayments() {
        repository.purge();
        executorMonitor.startMonitoring();
        return ResponseEntity.ok().build();
    }

    @PreDestroy
    public void shutdown() {
        if (executor.isShutdown()) return;
        executor.shutdownNow();
    }

}
