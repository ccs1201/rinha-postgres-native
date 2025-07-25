package br.com.ccs.rinha.service;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PaymentProcessorClientServiceReactive {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClientServiceReactive.class);

    private final JdbcPaymentRepository repository;
    private final String defaultUrl;
    private final String fallbackUrl;
    private final WebClient webClient;
    private final ArrayBlockingQueue<PaymentRequest> queue;
    private final int retries;
    private final Duration timeOut;

    public PaymentProcessorClientServiceReactive(
            JdbcPaymentRepository paymentRepository,
            WebClient webClient,
            @Value("${payment-processor.default.url}") String defaultUrl,
            @Value("${payment-processor.fallback.url}") String fallbackUrl) {

        this.repository = paymentRepository;

        this.defaultUrl = defaultUrl.concat("/payments");
        this.fallbackUrl = fallbackUrl.concat("/payments");
        this.webClient = webClient;
        this.queue = new ArrayBlockingQueue<>(10000, false);
        this.retries = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_MAX_RETRIES"));
        this.timeOut = Duration.ofMillis(Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_REQUEST_TIMEOUT")));
        var workers = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_WORKERS"));

        for (int i = 0; i < workers; i++) {
            startProcessQueue(i);
        }

        log.info("Default service URL: {}", this.defaultUrl);
        log.info("Fallback service URL: {}", this.fallbackUrl);
        log.info("Request timeout: {}", timeOut);
        log.info("Max retries: {}", retries);
        log.info("Workers: {}", workers);
    }

    public void processPayment(PaymentRequest paymentRequest) {
        var accepted = queue.offer(paymentRequest);
        if (!accepted) {
            log.error("Payment rejected by queue");
        }
    }

    private void startProcessQueue(int wokerIndex) {
        Thread.ofVirtual().name("payment-processor" + wokerIndex).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    processWithRetryReactive(queue.take());
                } catch (InterruptedException e) {
                    log.error("worker: {} has error: {}", Thread.currentThread().getName(), e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void processWithRetryReactive(PaymentRequest paymentRequest) {
        paymentRequest.requestedAt = OffsetDateTime.now(ZoneOffset.UTC);
        AtomicInteger attempts = new AtomicInteger();

        Mono.defer(() -> trySend(paymentRequest, attempts.getAndIncrement()))
                .repeatWhenEmpty(repeat -> repeat.take(retries))
                .subscribe(success -> {
                    if (Boolean.TRUE.equals(success)) {
                        saveAsync(paymentRequest);
                    } else {
                        queue.offer(paymentRequest);
                    }
                }, error -> {
                    log.error("Unexpected error on process payment", error);
                    queue.offer(paymentRequest);
                });
    }

    private Mono<Boolean> trySend(PaymentRequest request, int attempt) {
        return postToDefault(request)
                .flatMap(success -> {
                    if (Boolean.TRUE.equals(success)) return Mono.just(true);
                    return postToFallback(request);
                });
    }


    private Mono<Boolean> postToDefault(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultTrue();

        return webClient.post()
                .uri(defaultUrl)
                .bodyValue(paymentRequest.getJson())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> Mono.error(new RuntimeException("Erro HTTP")))
                .toBodilessEntity()
                .timeout(timeOut)
                .map(r -> true)
                .onErrorReturn(false);
    }

    private Mono<Boolean> postToFallback(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultFalse();

        return webClient.post()
                .uri(fallbackUrl)
                .bodyValue(paymentRequest.getJson())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> Mono.error(new RuntimeException("Erro HTTP")))
                .toBodilessEntity()
                .timeout(timeOut)
                .map(r -> true)
                .onErrorReturn(false);
    }

    private void saveAsync(PaymentRequest request) {
        repository.saveAsync(request);
    }
}