package br.com.ccs.rinha.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {

    private static final Logger log = LoggerFactory.getLogger(WebClientConfig.class);

    @Bean
    public WebClient webClient() {

        var connectionTimeOut = Integer.parseInt(System.getenv("REQUEST_CONNECTION_TIMEOUT"));
        var readTimeOut = Integer.parseInt(System.getenv("REQUEST_READ_TIMEOUT"));

        log.info("Connection timeout: {}", connectionTimeOut);
        log.info("Read timeout: {}", readTimeOut);

        ConnectionProvider provider = ConnectionProvider.builder("rinha")
                .maxConnections(400) // número máximo de conexões simultâneas
                .pendingAcquireMaxCount(5000) // tamanho máximo da fila
                .pendingAcquireTimeout(Duration.ofMillis(connectionTimeOut)) // tempo máximo para esperar por uma conexão
                .build();

        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeOut)
                .responseTimeout(Duration.ofMillis(readTimeOut))
                .doOnConnected(conn ->
                        conn.addHandlerLast(new ReadTimeoutHandler(readTimeOut, TimeUnit.MILLISECONDS))
                );

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();

//        HttpClient httpClient = HttpClient.create()
//                .keepAlive(true)
//                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeOut)
//                .responseTimeout(Duration.ofMillis(readTimeOut))
//                .doOnConnected(conn -> conn
//                        .addHandlerLast(new ReadTimeoutHandler(readTimeOut, TimeUnit.MILLISECONDS))
//                        .addHandlerLast(new WriteTimeoutHandler(readTimeOut, TimeUnit.MILLISECONDS)));
//
//        return WebClient.builder()
//                .clientConnector(new ReactorClientHttpConnector(httpClient))
//                .build();
    }
}
