package br.com.ccs.rinha.api.controller;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import br.com.ccs.rinha.service.PaymentProcessorClientServiceBlocking;
import jakarta.annotation.PreDestroy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
public class PaymentController {

    private final PaymentProcessorClientServiceBlocking client;
    private final JdbcPaymentRepository repository;
    private final ExecutorService executor;

    public PaymentController(PaymentProcessorClientServiceBlocking clientServiceBlocking,
                             JdbcPaymentRepository repository,
                             ThreadPoolExecutor executor) {

        this.client = clientServiceBlocking;
        this.repository = repository;
        this.executor = executor;
    }

    @PostMapping("/payments")
    public void createPayment(@RequestBody PaymentRequest paymentRequest) {
        executor.submit(() -> {
            paymentRequest.receivedAt = OffsetDateTime.now(ZoneOffset.UTC);
            client.processPayment(paymentRequest);
        }, executor);
    }

    @GetMapping("/payments-summary")
    public PaymentSummary getPaymentsSummary(@RequestParam(required = false) OffsetDateTime from,
                                             @RequestParam(required = false) OffsetDateTime to) {

        return CompletableFuture.supplyAsync(() -> repository.getSummary(from, to), executor).join();
    }

    @PostMapping("/purge-payments")
    public ResponseEntity<Void> purgePayments() {
        repository.purge();
        return ResponseEntity.ok().build();
    }

    @PreDestroy
    public void shutdown() {
        if (executor.isShutdown()) return;
        executor.shutdownNow();
    }

}
