package com.pickmytrade.ibapp;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.pickmytrade.ibapp.bussinesslogic.PlaceOrderService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

public class TradeServer {
    private static final Logger log = LoggerFactory.getLogger(TradeServer.class);
    private final PlaceOrderService placeOrderService;
    private final Gson gson = new Gson();
    private HttpServer server;

    public TradeServer(PlaceOrderService placeOrderService) {
        this.placeOrderService = placeOrderService;
    }

    public void start() throws IOException {
        try {
            // Create plain HTTP server
            server = HttpServer.create(new InetSocketAddress("localhost", 8880), 0);
            server.createContext("/place-trade", this::handlePlaceTrade);
            server.setExecutor(Executors.newFixedThreadPool(32)); // Thread pool for handling requests
            server.start();
            log.info("HTTP Trade Server started on http://localhost:8880/place-trade");
        } catch (Exception e) {
            log.error("Failed to start HTTP Trade Server: {}", e.getMessage(), e);
            throw new IOException("Failed to start HTTP Trade Server", e);
        }
    }

    public void stop() {
        if (server != null) {
            log.info("HTTP Trade Server stopped");
        }
    }

    private void handlePlaceTrade(HttpExchange exchange) {
        if ("POST".equals(exchange.getRequestMethod())) {
            try {
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                log.info("Received trade request: {}", requestBody);
                Map<String, Object> tradeData = gson.fromJson(requestBody, new TypeToken<Map<String, Object>>() {
                }.getType());
                log.debug("Parsed trade data: {}", tradeData);

                long startTime = System.currentTimeMillis();
                // Process trade asynchronously without blocking
                placeOrderService.placeTrade(tradeData).whenComplete((result, exception) -> {
                    try {
                        long duration = System.currentTimeMillis() - startTime;
                        log.info("Trade placement completed in {} ms with result: {}", duration, result);
                        String response;
                        if (exception != null) {
                            log.error("Error placing trade: {}", exception.getMessage(), exception);
                            response = gson.toJson(Map.of("success", false, "message", exception.getMessage()));
                            exchange.getResponseHeaders().set("Content-Type", "application/json");
                            exchange.sendResponseHeaders(500, response.length());
                            try (OutputStream os = exchange.getResponseBody()) {
                                os.write(response.getBytes());
                            }
                        } else {
                            response = gson.toJson(Map.of("success", result, "message", result ? "Trade placed successfully" : "Trade placement failed"));
                            exchange.getResponseHeaders().set("Content-Type", "application/json");
                            exchange.sendResponseHeaders(200, response.length());
                            try (OutputStream os = exchange.getResponseBody()) {
                                os.write(response.getBytes());
                            }
                        }
                    } catch (IOException ex) {
                        log.error("Error sending response: {}", ex.getMessage(), ex);
                    } finally {
                        exchange.close();
                    }
                });
            } catch (Exception e) {
                log.error("Error handling trade request: {}", e.getMessage(), e);
                String response = gson.toJson(Map.of("success", false, "message", e.getMessage()));
                try {
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(500, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                } catch (IOException ex) {
                    log.error("Error sending error response: {}", ex.getMessage(), ex);
                } finally {
                    exchange.close();
                }
            }
        } else {
            try {
                log.warn("Method not allowed: {}", exchange.getRequestMethod());
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            } catch (IOException e) {
                log.error("Error sending 405 response: {}", e.getMessage(), e);
            } finally {
                exchange.close();
            }
        }
    }
}