package br.com.ccs.rinha.service;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.repository.JdbcPaymentRepository;
import br.com.ccs.rinha.repository.R2dbcPaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class WebClientPaymentProcessorClientService {

    private static final Logger log = LoggerFactory.getLogger(WebClientPaymentProcessorClientService.class.getName());

    //    private final JdbcPaymentRepository repository;
    private final WebClient webClient;
    private final String defaultUrl;
    private final String fallbackUrl;
    private final ExecutorService executor;
    private final AtomicInteger maxRetryReaches = new AtomicInteger(0);
    private final R2dbcPaymentRepository r2dbcPaymentRepository;

    public WebClientPaymentProcessorClientService(
//            JdbcPaymentRepository repository,
            WebClient webClient,
            ExecutorService executor,
            R2dbcPaymentRepository r2dbcPaymentRepository,
            @Value("${payment-processor.default.url}") String defaultUrl,
            @Value("${payment-processor.fallback.url}") String fallbackUrl) {

//        this.repository = repository;
        this.webClient = webClient;
        this.defaultUrl = defaultUrl + "/payments";
        this.fallbackUrl = fallbackUrl + "/payments";
        this.executor = executor;
        this.r2dbcPaymentRepository = r2dbcPaymentRepository;
    }

    public void processPayment(PaymentRequest request) {
        processWithRetry(request, 0);
    }

    private void processWithRetry(PaymentRequest request, int retryCount) {
        if (retryCount >= 3) {
            log.error("Max retries reaches {}", maxRetryReaches.incrementAndGet());
            return;
        }

        postToDefault(request, retryCount);
    }

    private void postToDefault(PaymentRequest request, int retryCount) {
        request.setDefaultTrue();

        webClient.post()
                .uri(defaultUrl)
                .bodyValue(request)
                .retrieve()
                .toBodilessEntity()
                .then(r2dbcPaymentRepository.save(request))
                .doOnError(e -> {
                    log.error("Default error", e);
                    postToFallback(request, retryCount);
                })
                .subscribe();
    }

    private void postToFallback(PaymentRequest request, int retryCount) {
        request.setDefaultFalse();

        webClient.post()
                .uri(fallbackUrl)
                .bodyValue(request)
                .retrieve()
                .toBodilessEntity()
                .then(r2dbcPaymentRepository.save(request))
                .doOnError(e -> {
                    log.error("Fallback error", e);
                    executor.submit(() ->
                            postToDefault(request, retryCount + 1), executor);
                })
                .subscribe();
    }
}
