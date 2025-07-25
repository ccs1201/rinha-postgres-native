package br.com.ccs.rinha.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient() {

        var maxConnection = Integer.parseInt(System.getenv("WEBCLIENT_MAX_CONNECTION"));

        var provider = ConnectionProvider.builder("burst-client")
                .maxConnections(maxConnection)
                .pendingAcquireTimeout(Duration.ofSeconds(10))
                .build();

        var httpClient = HttpClient.create(provider)
                .keepAlive(true)
                .compress(false)
                .wiretap(false);

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}