package br.com.ccs.rinha.service;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PaymentProcessorClientService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClientService.class);

    private final JdbcPaymentRepository repository;
    //    private final RestTemplate restTemplate;
    private final String defaultUrl;
    private final String fallbackUrl;
    private final ThreadPoolExecutor executorService;
    private final AtomicInteger rejectedRetries = new AtomicInteger(0);
    private final WebClient webClient;
    private final ArrayBlockingQueue<PaymentRequest> queue;


    public PaymentProcessorClientService(
            JdbcPaymentRepository paymentRepository,
//            RestTemplate restTemplate,
            WebClient webClient,
            ThreadPoolExecutor executorService,
            @Value("${payment-processor.default.url}") String defaultUrl,
            @Value("${payment-processor.fallback.url}") String fallbackUrl) {

        this.repository = paymentRepository;

        this.defaultUrl = defaultUrl.concat("/payments");
        this.fallbackUrl = fallbackUrl.concat("/payments");
//        this.restTemplate = restTemplate;
        this.webClient = webClient;
        this.executorService = executorService;
        this.queue = new ArrayBlockingQueue<>(10000, false);
        startProcessQueue();


        log.info("Default service URL: {}", this.defaultUrl);
        log.info("Fallback service URL: {}", this.fallbackUrl);
    }

    public void processPayment(PaymentRequest paymentRequest) {
        var accepted = queue.offer(paymentRequest);
        if (!accepted) {
            log.error("Payment rejected by queue");
        }
    }

    private void startProcessQueue() {
        CompletableFuture.runAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    PaymentRequest payment = queue.take();
                    postToDefault(payment);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private void postToDefault(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultTrue();
        paymentRequest.requestedAt = OffsetDateTime.now(ZoneOffset.UTC);

        webClient.post()
                .uri(defaultUrl)
                .bodyValue(paymentRequest)
                .retrieve()
                .toBodilessEntity()
                .doOnSuccess(r -> saveAsync(paymentRequest))
                .doOnError(t -> {
//                    log.error("Default error msg: {}", e.getMessage());
                    postToFallback(paymentRequest);
                })
                .onErrorResume(e -> Mono.empty())
                .subscribe();
    }

    private void postToFallback(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultFalse();
        paymentRequest.requestedAt = OffsetDateTime.now(ZoneOffset.UTC);

        webClient.post()
                .uri(fallbackUrl)
                .bodyValue(paymentRequest)
                .retrieve()
                .toBodilessEntity()
                .doOnSuccess(r -> saveAsync(paymentRequest))
                .doOnError(t -> processPayment(paymentRequest))
                .onErrorResume(e -> Mono.empty())
                .subscribe();
    }

    private void saveAsync(PaymentRequest request) {
        repository.saveAsync(request);
    }
}