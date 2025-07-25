package br.com.ccs.rinha.service;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ArrayBlockingQueue;

//@Service
public class PaymentProcessorClientServiceBlocking {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClientServiceBlocking.class);

    private final JdbcPaymentRepository repository;
    private final String defaultUrl;
    private final String fallbackUrl;
    private final WebClient webClient;
    private final ArrayBlockingQueue<PaymentRequest> queue;
    private final int retries;
    private final int timeOut;


    public PaymentProcessorClientServiceBlocking(
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
        this.timeOut = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_REQUEST_TIMEOUT"));
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

    private void startProcessQueue(int wokerIndex) {
        log.info("Starting payment-processor-worker-{}", wokerIndex);
        Thread.ofVirtual().name("payment-processor" + wokerIndex).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    processWithRetry(queue.take());
                } catch (InterruptedException e) {
                    log.error("worker: {} has error: {}", Thread.currentThread().getName(), e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        });

        log.info("payment-processor-worker-{} started", wokerIndex);
    }

    public void processPayment(PaymentRequest paymentRequest) {
        var accepted = queue.offer(paymentRequest);
        if (!accepted) {
            log.error("Payment rejected by queue");
        }
    }

    private void processWithRetry(PaymentRequest paymentRequest) {
        paymentRequest.requestedAt = OffsetDateTime.now(ZoneOffset.UTC);
        for (int i = 0; i < retries; i++) {
            if (Boolean.TRUE.equals(postToDefault(paymentRequest))) {
                saveAsync(paymentRequest);
                return;
            }

            if (Boolean.TRUE.equals(postToFallback(paymentRequest))) {
                saveAsync(paymentRequest);
                return;
            }
        }
        queue.offer(paymentRequest);
    }


    private Boolean postToDefault(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultTrue();

        return webClient.post()
                .uri(defaultUrl)
                .bodyValue(paymentRequest.getJson())
                .exchangeToMono(clientResponse ->
                        Mono.just(clientResponse.statusCode().is2xxSuccessful()))
                .timeout(Duration.ofMillis(timeOut))
                .onErrorReturn(Boolean.FALSE)
                .block();
    }

    private Boolean postToFallback(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultFalse();

        return webClient.post()
                .uri(fallbackUrl)
                .bodyValue(paymentRequest.getJson())
                .exchangeToMono(clientResponse ->
                        Mono.just(clientResponse.statusCode().is2xxSuccessful())
                )
                .timeout(Duration.ofMillis(timeOut))
                .onErrorReturn(Boolean.FALSE)
                .block();
    }

    private void saveAsync(PaymentRequest request) {
        repository.saveAsync(request);
    }
}