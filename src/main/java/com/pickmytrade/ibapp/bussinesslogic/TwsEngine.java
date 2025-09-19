package com.pickmytrade.ibapp.bussinesslogic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.*;
import com.ib.controller.ApiController;
import com.ib.controller.Bar;
import com.ib.controller.Position;
import com.pickmytrade.ibapp.db.DatabaseConfig;
import com.pickmytrade.ibapp.db.entities.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.ib.controller.ApiController.IContractDetailsHandler;
import com.ib.controller.ApiController.ISecDefOptParamsReqHandler;

import static com.pickmytrade.ibapp.config.Config.log;

public class TwsEngine {
    private static final Logger log = LoggerFactory.getLogger(TwsEngine.class);
    private final ApiController controller;
    private final ExecutorService executor = Executors.newFixedThreadPool(16);
    public static ExecutorService orderStatusExecutor = Executors.newSingleThreadExecutor();
    public static final AtomicBoolean orderStatusProcessingStarted = new AtomicBoolean(false); // Flag to track if processing is started
    private final AtomicBoolean stopFlag = new AtomicBoolean(false);
    private final Gson gson = new GsonBuilder()
            .serializeNulls()
            .create();
    private volatile boolean isConnected = false;
    private final Map<Contract, ApiController.ITopMktDataHandler> marketDataHandlers = new HashMap<>();
    private volatile boolean orderIdReceived = false;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private final List<Map<String, Object>> positions = Collections.synchronizedList(new ArrayList<>());
    private final List<Map.Entry<Order, Contract>> openOrders = Collections.synchronizedList(new ArrayList<>());
    private static final ArrayBlockingQueue<Map<String, Object>> orderStatusQueue = new ArrayBlockingQueue<>(1000); // Static queue
    private final Map<Integer, CachedStrikeData> strikeCache = Collections.synchronizedMap(new HashMap<>());


    public TwsEngine() {
        controller = new ApiController(new CustomConnectionHandler(), System.out::println, System.err::println);
        startOrderStatusProcessing(); // Start processing when the instance is created
    }


    public void startOrderStatusProcessing() {
        if (orderStatusProcessingStarted.compareAndSet(false, true)) {
            log.info("Starting order status queue processing");
            orderStatusExecutor.submit(this::processOrderStatusQueue);
        } else {
            log.debug("Order status queue processing already started");
        }
    }

    public static class OrderExecutionResult {
        private final Order order;
        private final CompletableFuture<Order> future;

        public OrderExecutionResult(Order order, CompletableFuture<Order> future) {
            this.order = order;
            this.future = future;
        }

        public Order getOrder() {
            return order;
        }

        public CompletableFuture<Order> getFuture() {
            return future;
        }
    }

    public CountDownLatch getConnectionLatch() {
        return connectionLatch;
    }

    private class CustomConnectionHandler implements ApiController.IConnectionHandler {
        @Override
        public void connected() {
            isConnected = true;
            log.info("Connected to TWS");
            connectionLatch.countDown();
        }

        @Override
        public void disconnected() {
            isConnected = false;
            log.warn("Disconnected from TWS");
            synchronized (positions) {
                positions.clear();
            }
            synchronized (openOrders) {
                openOrders.clear();
            }
            synchronized (orderStatusQueue) {
                orderStatusQueue.clear(); // Clear queue on disconnect to avoid stale data
            }
        }

        @Override
        public void accountList(List<String> list) {
            log.info("Account list received: {}", list);
            if (!list.isEmpty()) {
                try {
                    DatabaseConfig.emptyAccountDataTable();
                } catch (SQLException e) {
                    log.info("Error while getting and saving account ids");
                }
            }
            for (String accountId : list) {
                AccountData accountData = new AccountData();
                accountData.setAccountId(accountId);
                accountData.setData("Account information for " + accountId);
                try {
                    DatabaseConfig.saveAccountData(accountData);
                } catch (SQLException e) {
                    log.error("Failed to save account {}: {}", accountId, e.getMessage());
                }
            }
        }

        @Override
        public void error(Exception e) {
            log.error("TWS error: {}", e.getMessage(), e);
        }

        @Override
        public void message(int id, int errorCode, String errorMsg, String advancedOrderRejectJson) {
            log.warn("TWS message: id={}, errorCode={}, msg={}, advanced={}", id, errorCode, errorMsg, advancedOrderRejectJson);
            errorFunc(id, errorCode, errorMsg, null);
        }

        @Override
        public void show(String string) {
            log.info("TWS show: {}", string);
        }
    }

    private void processOrderStatusQueue() {


        while (!stopFlag.get()) {
            try {
                // check if 5 minutes have passed

                Map<String, Object> statusData = orderStatusQueue.poll(1, TimeUnit.SECONDS);
                if (statusData != null) {
                    orderStatusLoop(
                            (int) statusData.get("orderId"),
                            (OrderStatus) statusData.get("status"),
                            (Decimal) statusData.get("filled"),
                            (Decimal) statusData.get("remaining"),
                            (double) statusData.get("avgFillPrice"),
                            (int) statusData.get("permId"),
                            (int) statusData.get("parentId"),
                            (double) statusData.get("lastFillPrice"),
                            (int) statusData.get("clientId"),
                            (String) statusData.get("whyHeld")
                    );
                }
            } catch (Exception e) {
                log.error("Order status queue processing interrupted: {}", e.getMessage());
            } catch (Error e) {
                log.error("Severe error in order status queue processing: {}", e.getMessage());
                throw e;
            }
        }
        log.info("Order status queue processing stopped");
    }


    public ApiController getController() {
        return controller;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void twsConnect() {
        log.info("Attempting to connect to TWS");
        controller.connect("127.0.0.1", 7497, 0, "+PACEAPI");
        controller.client().setAsyncEConnect(true);
        executor.submit(() -> startTwsSubscriptions());
    }

    public void startTwsSubscriptions() {
        try {
            log.debug("Waiting for TWS connection in startTwsSubscriptions");
            connectionLatch.await();
            if (isConnected) {
                log.info("Subscribing to TWS events");
                controller.reqAccountUpdates(false, null, new AccountHandler());
                controller.reqPositions(new PositionHandler(null));
                controller.reqLiveOrders(new LiveOrderHandler(new CompletableFuture<>()));
                // Do not resubmit processOrderStatusQueue here; it's already running
            } else {
                log.warn("Not connected to TWS, skipping subscriptions");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for connection: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public List<Map<String, Object>> getAllPositions() {
        synchronized (positions) {
            return new ArrayList<>(positions);
        }
    }

    public List<Map.Entry<Order, Contract>> getAllOpenOrders() {
        synchronized (openOrders) {
            return new ArrayList<>(openOrders);
        }
    }

    public void disconnect() {
        log.info("Initiating TWS disconnection and cleanup");

        try {
            marketDataHandlers.forEach((contract, handler) -> {
                try {
                    controller.cancelTopMktData(handler);
                    log.info("Cancelled market data for contract: {}", contract);
                } catch (Exception e) {
                    log.error("Error cancelling market data for contract {}: {}", contract, e.getMessage());
                }
            });
            marketDataHandlers.clear();

            try {
                controller.reqAccountUpdates(false, null, null);
                log.info("Stopped account updates");
            } catch (Exception e) {
                log.error("Error stopping account updates: {}", e.getMessage());
            }

            try {
                controller.cancelPositions(null);
                log.info("Stopped position updates");
            } catch (Exception e) {
                log.error("Error stopping position updates: {}", e.getMessage());
            }

            if (isConnected) {
                controller.disconnect();
                isConnected = false;
                log.info("Disconnected from TWS");
            }

            try {
                executor.shutdown();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    log.warn("Executor did not terminate gracefully, forced shutdown");
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                log.error("Executor shutdown interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            log.error("Unexpected error during disconnection: {}", e.getMessage(), e);
        }

        log.info("TWS disconnection and cleanup completed");
    }

    private class AccountHandler implements ApiController.IAccountHandler {
        @Override
        public void accountValue(String account, String key, String value, String currency) {
            log.debug("Account value: account={}, key={}, value={}, currency={}", account, key, value, currency);
        }

        @Override
        public void accountTime(String timeStamp) {
            log.debug("Account time: {}", timeStamp);
        }

        @Override
        public void accountDownloadEnd(String account) {
            log.info("Account download ended for: {}", account);
        }

        @Override
        public void updatePortfolio(Position position) {
            log.info("Portfolio update: account={}, contract={}, position={}, avgCost={}",
                    position.account(), position.contract().toString(), position.position(), position.averageCost());
        }
    }

    private class PositionHandler implements ApiController.IPositionHandler {
        private final CompletableFuture<List<Map<String, Object>>> future;

        public PositionHandler(CompletableFuture<List<Map<String, Object>>> future) {
            this.future = future;
        }

        @Override
        public void position(String account, Contract contract, Decimal pos, double avgCost) {
            Map<String, Object> positionData = new HashMap<>();
            positionData.put("contract", contract);
            positionData.put("account", account);
            positionData.put("position", pos);
            positionData.put("avgCost", avgCost);

            synchronized (positions) {
                positions.removeIf(posData -> {
                    Contract posContract = (Contract) posData.get("contract");
                    boolean isSameAccount = posData.get("account").equals(account);
                    boolean isSameSymbol = posContract.symbol().equals(contract.symbol());
                    boolean shouldRemove = isSameAccount && isSameSymbol;

                    // For OPT or FOP, also check the right (CALL or PUT) and lastTradeDateOrContractMonth
                    if (shouldRemove && (String.valueOf(posContract.secType()).equals("OPT") || String.valueOf(posContract.secType()).equals("FOP"))) {
                        String posRight = String.valueOf(posContract.right());
                        String contractRight = String.valueOf(contract.right());
                        boolean isSameMaturity = String.valueOf(posContract.lastTradeDateOrContractMonth()).equals(
                                String.valueOf(contract.lastTradeDateOrContractMonth()));
                        boolean isSameStrike = posContract.strike() == contract.strike();
                        if (posRight != null && contractRight != null && isSameMaturity && isSameStrike) {
                            // Only remove if rights are the same (e.g., both CALL or both PUT) and maturity dates match
                            shouldRemove = posRight.substring(0, 1).equalsIgnoreCase(contractRight.substring(0, 1));
                        } else {
                            // If either right is null or maturity dates don't match, don't remove
                            shouldRemove = false;
                        }
                    }
                    // For FUT and STK, only check account and symbol (no maturity or right check)

                    return shouldRemove;
                });

                if (pos.longValue() != 0) {
                    positions.add(positionData);
                }
            }
        }

        @Override
        public void positionEnd() {
            if (future != null) {
                synchronized (positions) {
                    future.complete(new ArrayList<>(positions));
                }
            }
        }
    }

    public Contract createContract(String instType, String symbol, String exchange, String currency,
                                   Double strike, String right, String baseSymbol, String maturityDate,
                                   String tradingClass) {
        Contract contract = new Contract();
        instType = instType.toUpperCase();
        symbol = symbol.toUpperCase();

        switch (instType) {
            case "STK":
                contract.symbol(symbol);
                contract.secType(Types.SecType.STK);
                contract.exchange(exchange.toUpperCase());
                contract.currency(currency.toUpperCase());
                break;
            case "CASH":
                contract.symbol(symbol);
                contract.secType(Types.SecType.CASH);
                contract.exchange("IDEALPRO");
                contract.currency(currency.toUpperCase());
                break;
            case "CFD":
                contract.symbol(symbol);
                contract.secType(Types.SecType.CFD);
                contract.exchange(exchange.toUpperCase());
                contract.currency(currency.toUpperCase());
                break;
            case "FUT":
                contract.symbol(symbol);
                contract.secType(Types.SecType.FUT);
                contract.currency(currency.toUpperCase());
                contract.exchange(exchange.toUpperCase());
                contract.lastTradeDateOrContractMonth(maturityDate);
                contract.tradingClass(tradingClass);
                if ("SPX".equals(symbol)) {
                    contract.tradingClass("SPXW");
                }
                break;
            case "OPT":
                contract.symbol(symbol);
                contract.secType(Types.SecType.OPT);
                contract.exchange(exchange.toUpperCase());
                contract.lastTradeDateOrContractMonth(maturityDate);
                contract.strike(strike != null ? strike : 0);
                contract.right(right != null ? right.toUpperCase() : "");
                contract.currency(currency.toUpperCase());
                if ("SPX".equals(symbol)) {
                    contract.tradingClass("SPXW");

                }
                break;
            case "FOP":
                contract.symbol(symbol);
                contract.secType(Types.SecType.FOP);
                contract.exchange(exchange.toUpperCase());
                contract.lastTradeDateOrContractMonth(maturityDate);
                contract.strike(strike != null ? strike : 0);
                contract.right(right != null ? right.toUpperCase() : "");
                contract.currency(currency.toUpperCase());
                contract.tradingClass(tradingClass);
                break;
            default:
                log.error("Unsupported instrument type: {}", instType);
                throw new IllegalArgumentException("Unsupported instrument type: " + instType);
        }
        return contract;
    }

    // Core function to get closest strike
    public Double getClosestStrike(int conId, double strike) {
        try {
            LocalDate today = LocalDate.now();
            Contract contract = new Contract();
            contract.conid(conId);
            // Check cache
            CachedStrikeData cached = strikeCache.get(conId);
            if (cached != null && cached.getFetchDate().equals(today)) {
                List<Double> cachedStrikes = cached.getStrikes();
                Double closestStrike = cachedStrikes.stream()
                        .filter(s -> s <= strike)
                        .max(Double::compareTo)
                        .orElse(strike);

                log.info("Using cached strikes for conId={}: Closest strike={}, Available={}",
                        conId, closestStrike, cachedStrikes);
                return closestStrike;
            }

            // Fetch contract details
            List<ContractDetails> details = reqContractDetailsSync(contract);
            if (details == null || details.isEmpty()) {
                log.warn("No contract details found for contract ID: {}, using input strike: {}", conId, strike);
                return strike;
            }

            ContractDetails contractDetails = details.get(0);
            String underlyingSecType = contractDetails.contract().secType().toString();
            String underlyingSymbol = contractDetails.contract().symbol();
            String futFopExchange = "";

            log.info("Contract details retrieved: symbol={}, secType={}", underlyingSymbol, underlyingSecType);

            List<Double> strikes = reqSecDefOptParamsSync(underlyingSymbol, conId, underlyingSecType, futFopExchange);
            if (strikes == null || strikes.isEmpty()) {
                log.warn("No strikes available for contract ID: {}, using input strike: {}", conId, strike);
                return strike;
            }

            // Cache result
            strikeCache.put(conId, new CachedStrikeData(strikes, today));

            Double closestStrike = strikes.stream()
                    .filter(s -> s <= strike)
                    .max(Double::compareTo)
                    .orElse(strike);

            log.info("Fetched new strikes: Closest strike={}, All available={}", closestStrike, strikes);
            return closestStrike;

        } catch (Exception e) {
            log.error("Error fetching closest strike for contract ID {}: {}, using input strike: {}",
                    conId, e.getMessage(), strike);
            return strike;
        }
    }

    // Blocking contract detail fetch
    public List<ContractDetails> reqContractDetailsSync(Contract contract) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<ContractDetails> detailsList = new ArrayList<>();

        log.info("Requesting contract details for contract: {}", contract.toString());

        controller.reqContractDetails(contract, new IContractDetailsHandler() {
            @Override
            public void contractDetails(List<ContractDetails> details) {
                detailsList.addAll(details);
            }


            public void contractDetailsEnd() {
                log.info("Received {} contract details for contract: {}", detailsList.size(), contract.toString());
                latch.countDown();
            }
        });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            log.warn("Timeout while waiting for contract details for {}", contract.toString());
        }

        return detailsList;
    }



    // Blocking strike fetch
    public List<Double> reqSecDefOptParamsSync(String underlyingSymbol, int underlyingConId,
                                               String underlyingSecType, String futFopExchange) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Double> allStrikes = new HashSet<>();

        log.info("Requesting option chain strikes for underlyingSymbol={}, conId={}, secType={}, exchange={}",
                underlyingSymbol, underlyingConId, underlyingSecType, futFopExchange);

        controller.reqSecDefOptParams(underlyingSymbol, futFopExchange, underlyingSecType, underlyingConId,
                new ISecDefOptParamsReqHandler() {
                    @Override
                    public void securityDefinitionOptionalParameter(String exchange, int underlyingConId,
                                                                    String tradingClass, String multiplier,
                                                                    Set<String> expirations, Set<Double> strikes) {
                        allStrikes.addAll(strikes);
                        log.info("Received option chain: exchange={}, conId={}, tradingClass={}, strikes={}",
                                exchange, underlyingConId, tradingClass, strikes);
                    }

                    @Override
                    public void securityDefinitionOptionalParameterEnd(int reqId) {
                        log.info("Completed option chain request for reqId={}, unique strikes count={}", reqId, allStrikes.size());
                        latch.countDown();
                    }
                });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            log.warn("Timeout while waiting for option strikes");
        }

        return allStrikes.stream().sorted().collect(Collectors.toList());
    }

    // Internal cache structure
    public static class CachedStrikeData {
        private final List<Double> strikes;
        private final LocalDate fetchDate;

        public CachedStrikeData(List<Double> strikes, LocalDate fetchDate) {
            this.strikes = strikes;
            this.fetchDate = fetchDate;
        }

        public List<Double> getStrikes() {
            return strikes;
        }

        public LocalDate getFetchDate() {
            return fetchDate;
        }
    }

    // Optional future-proof cache key structure (not used yet)
    public static class StrikeCacheKey {
        private final int conId;

        public StrikeCacheKey(int conId) {
            this.conId = conId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StrikeCacheKey)) return false;
            StrikeCacheKey that = (StrikeCacheKey) o;
            return conId == that.conId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(conId);
        }
    }



    public Object createOrder(String tradeType, String action, Integer quantity, Double stopPrice, Double limitPrice,
                              Double tpPrice, Double slPrice, Double slDollar, Double tpDollar, Double slPercentage,
                              Double tpPercentage, Double trailingAmount, String account, Boolean reverseOrderClose,
                              Boolean duplicatePositionAllow, Double entrylmtPriceOffset, Double entrytrailingAmount , Double sllmtPriceOffset ) {
        if (slPrice != null || tpPrice != null || trailingAmount != null || slDollar != null || tpDollar != null ||
                slPercentage != null || tpPercentage != null || entrytrailingAmount != null || entrylmtPriceOffset != null || sllmtPriceOffset != null) {
            return makeBracketOrder(action, quantity, limitPrice, tpPrice, slPrice, tradeType.toUpperCase(),
                    trailingAmount, slDollar, tpDollar, slPercentage, tpPercentage, entrylmtPriceOffset, entrytrailingAmount , sllmtPriceOffset);
        } else {
            Order order = new Order();
            tradeType = tradeType.toUpperCase();
            order.action(action.toUpperCase());
            order.totalQuantity(Decimal.parse(String.valueOf(quantity)));
            order.account(account);
            switch (tradeType) {
                case "SNAPMKT":
                    order.orderType("SNAP MKT");
                    order.auxPrice(stopPrice != null ? stopPrice : 0);
                    break;
                case "PEGMKT":
                    order.orderType("PEG MKT");
                    order.auxPrice(stopPrice != null ? stopPrice : 0);
                    break;
                case "MKT":
                    order.orderType("MKT");
                    break;
                case "LMT":
                    order.orderType("LMT");
                    order.lmtPrice(limitPrice != null ? limitPrice : 0);
                    break;
                case "MOC":
                    order.orderType("MOC");
                    break;
                case "TRAIL LIMIT":
                    order.trailStopPrice(limitPrice);
                    order.lmtPriceOffset(entrylmtPriceOffset);
                    order.orderType("TRAIL LIMIT");
                    order.auxPrice(entrytrailingAmount);
                    break;
                default:
                    log.error("Unsupported trade type: {}", tradeType);
                    throw new IllegalArgumentException("Unsupported trade type: " + tradeType);
            }
            return order;
        }
    }

    private List<Order> makeBracketOrder(String action, int quantity, Double entryPrice, Double takeProfitPrice,
                                         Double stopLossPrice, String orderType, Double trailingAmount,
                                         Double slDollar, Double tpDollar, Double slPercentage, Double tpPercentage, Double entrylmtPriceOffset, Double entrytrailingAmount , Double sllmtPriceOffset
    ) {
        List<Order> orders = new ArrayList<>();
        Order parentOrder = new Order();
        parentOrder.action(action);
        parentOrder.orderType(orderType);
        parentOrder.totalQuantity(Decimal.parse(String.valueOf(quantity)));
        if ("LMT".equals(orderType) && entryPrice != null) {
            parentOrder.lmtPrice(entryPrice);
        } else if ("TRAIL LIMIT".equals(orderType)) {
            parentOrder.trailStopPrice(entryPrice);
            parentOrder.lmtPriceOffset(entrylmtPriceOffset);
            parentOrder.auxPrice(entrytrailingAmount);
        }
        parentOrder.transmit(true);
        orders.add(parentOrder);

        if (takeProfitPrice != null || tpDollar != null || tpPercentage != null) {
            Order tpOrder = new Order();
            tpOrder.action("BUY".equals(action) ? "SELL" : "BUY");
            tpOrder.orderType("LMT");
            tpOrder.totalQuantity(Decimal.parse(String.valueOf(quantity)));
            tpOrder.lmtPrice(takeProfitPrice != null ? takeProfitPrice : 0);
//            tpOrder.parentId(parentOrder.orderId());
            orders.add(tpOrder);

        }

        if (takeProfitPrice == null && tpDollar == null && tpPercentage == null) {
            orders.add(null);

        }

        if (stopLossPrice != null || trailingAmount != null || slDollar != null || slPercentage != null) {
            Order slOrder = new Order();
            slOrder.action("BUY".equals(action) ? "SELL" : "BUY");
            if (trailingAmount != null && trailingAmount != 0 && sllmtPriceOffset != null && sllmtPriceOffset != 0) {
                slOrder.orderType("TRAIL LIMIT");
                parentOrder.trailStopPrice(0);
                parentOrder.lmtPriceOffset(sllmtPriceOffset);
                parentOrder.auxPrice(trailingAmount);
            } else {
                slOrder.orderType("STP");
                slOrder.auxPrice(stopLossPrice != null ? stopLossPrice : 0);
            }
            slOrder.totalQuantity(Decimal.parse(String.valueOf(quantity)));
//            slOrder.parentId(parentOrder.orderId());
            slOrder.transmit(true);
            orders.add(slOrder);
        }

        if (stopLossPrice == null && trailingAmount == null && slDollar == null && slPercentage == null) {
            orders.add(null);
        }
        return orders;
    }

    public OrderExecutionResult executeOrder(Contract contract, Order order) {
        CompletableFuture<Order> future = new CompletableFuture<>();
        controller.placeOrModifyOrder(contract, order, new OrderHandler(future, order, contract));
        return new OrderExecutionResult(order, future);
    }

    private class OrderHandler implements ApiController.IOrderHandler {
        private final CompletableFuture<Order> future;
        private final Order order;
        private final Contract contract;

        public OrderHandler(CompletableFuture<Order> future, Order order, Contract contract) {
            this.future = future;
            this.order = order;
            this.contract = contract;
        }

        @Override
        public void orderState(OrderState orderState, Order order) {
            log.debug("Order state for {}: {}", order.orderId(), orderState.status());
        }

        @Override
        public void orderStatus(OrderStatus status, Decimal filled, Decimal remaining, double avgFillPrice, int permId,
                                int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
            log.info("order status in orderStatus {}", status);
        }

        @Override
        public void handle(int errorCode, String errorMsg) {
            if (errorCode != 0) {
                log.error("Order error for {}: {} - {}", order.orderId(), errorCode, errorMsg);
                errorFunc(order.orderId(), errorCode, errorMsg, contract);
            }
        }
    }

    private class LiveOrderHandler implements ApiController.ILiveOrderHandler {
        private final CompletableFuture<List<Map.Entry<Order, Contract>>> future;

        public LiveOrderHandler(CompletableFuture<List<Map.Entry<Order, Contract>>> future) {
            this.future = future;
        }

        @Override
        public void openOrder(Contract contract, Order order, OrderState orderState) {
            Map.Entry<Order, Contract> entry = new AbstractMap.SimpleEntry<>(order, contract);
            synchronized (openOrders) {
                openOrders.removeIf(e -> e.getKey().orderId() == order.orderId());
                openOrders.add(entry);
            }
            log.info("Updated open order: orderId={}, contract={}", order.orderId(), contract.toString());
        }

        @Override
        public void openOrderEnd() {
            if (future != null) {
                synchronized (openOrders) {
                    future.complete(new ArrayList<>(openOrders));
                }
            }
        }

        @Override
        public void orderStatus(int orderId, OrderStatus status, Decimal filled, Decimal remaining,
                                double avgFillPrice, int permId, int parentId, double lastFillPrice,
                                int clientId, String whyHeld, double mktCapPrice) {
            log.info("Live order status update for order {}: {}", orderId, status);

            Map<String, Object> statusData = new HashMap<>();
            statusData.put("orderId", orderId);
            statusData.put("status", status);
            statusData.put("filled", filled);
            statusData.put("remaining", remaining);
            statusData.put("avgFillPrice", avgFillPrice);
            statusData.put("permId", permId);
            statusData.put("parentId", parentId);
            statusData.put("lastFillPrice", lastFillPrice);
            statusData.put("clientId", clientId);
            statusData.put("whyHeld", whyHeld);

            try {

                orderStatusQueue.put(statusData);
                log.info("Enqueued order status for orderId={}: {}", orderId, status);
                log.info("order status queue after adding orderId={} size={} orderStatusQueue={}", orderId, orderStatusQueue.size(), orderStatusQueue);
            } catch (Exception e) {
                log.error("Failed to add order status to queue for orderId={}: {}", orderId, e.getMessage());
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void handle(int id, int errorCode, String errorMsg) {
            log.error("Open order error: {} - {}", errorCode, errorMsg);
        }
    }



    public void cancelTrade(Order order) {
        OrderCancel orderCancel = new OrderCancel();
        controller.cancelOrder(order.orderId(), orderCancel, new ApiController.IOrderCancelHandler() {
            @Override
            public void handle(int errorCode, String errorMsg) {
                if (errorCode != 0) {
                    log.error("Cancel error for order {}: {} - {}", order.orderId(), errorCode, errorMsg);
                }
            }

            @Override
            public void orderStatus(String status) {
                log.info("Order orderStatus {} cancel status: {}", order.orderId(), status);
            }
        });
    }

    public CompletableFuture<Map<String, Double>> getOptionDetails(Contract contract) {
        CompletableFuture<Map<String, Double>> future = new CompletableFuture<>();
        TopMktDataHandler handler = new TopMktDataHandler(future, contract);
        controller.reqTopMktData(contract, "", false, false, handler);
        marketDataHandlers.put(contract, handler);
        return future;
    }

    private class TopMktDataHandler implements ApiController.ITopMktDataHandler {
        private final CompletableFuture<Map<String, Double>> future;
        private final Contract contract;
        private int attempts = 0;
        private double bid = Double.NaN;
        private double volume = Double.NaN;

        public TopMktDataHandler(CompletableFuture<Map<String, Double>> future, Contract contract) {
            this.future = future;
            this.contract = contract;
        }

        @Override
        public void tickPrice(TickType tickType, double price, TickAttrib attribs) {
            if (tickType == TickType.BID) {
                bid = price;
            }
            checkCompletion();
        }

        @Override
        public void tickSize(TickType tickType, Decimal size) {
            if (tickType == TickType.VOLUME) {
                volume = size.longValue();
            }
            checkCompletion();
        }

        private void checkCompletion() {
            if ((!Double.isNaN(bid) && !Double.isNaN(volume)) || attempts >= 50) {
                controller.cancelTopMktData(this);
                future.complete(Map.of("bid", bid, "volume", volume));
            }
            attempts++;
        }

        @Override
        public void tickString(TickType tickType, String value) {
        }

        @Override
        public void tickSnapshotEnd() {
            if (!future.isDone()) {
                future.complete(Map.of("bid", bid, "volume", volume));
            }
        }

        @Override
        public void marketDataType(int marketDataType) {
        }

        @Override
        public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
        }
    }

    public CompletableFuture<Map<String, Double>> reqMktData(Contract contract) {
        CompletableFuture<Map<String, Double>> future = new CompletableFuture<>();
        TopMktDataHandler handler = new TopMktDataHandler(future, contract);
        controller.reqTopMktData(contract, "100", false, false, handler);
        marketDataHandlers.put(contract, handler);
        return future;
    }

    public void unsubsMktData(Contract contract) {
        ApiController.ITopMktDataHandler handler = marketDataHandlers.remove(contract);
        if (handler != null) {
            controller.cancelTopMktData(handler);
        }
        try {
            Thread.sleep(2000);
            ErrorLog error = DatabaseConfig.getErrorData(contract.toString());
            if (error != null) log.info(error.toString());
        } catch (Exception e) {
            log.error("Unsubscribe error: {}", e.getMessage());
        }
    }

    public boolean getIbRollover(List<Integer> conIds) {
        log.warn("getIbRollover is not supported in TWS API 10.30.01");
        return false;
    }

    public CompletableFuture<double[]> reqLastCandle(Contract contract, String durationStr, String barSizeSetting) {
        CompletableFuture<double[]> future = new CompletableFuture<>();
        int duration = parseDuration(durationStr);
        Types.DurationUnit durationUnit = parseDurationUnit(durationStr);
        Types.BarSize barSize = Types.BarSize.valueOf(barSizeSetting.replace(" ", "_"));
        controller.reqHistoricalData(contract, "", duration, durationUnit, barSize, Types.WhatToShow.TRADES, true, false,
                new HistoricalDataHandler(future));
        return future;
    }

    private int parseDuration(String durationStr) {
        String[] parts = durationStr.split(" ");
        return Integer.parseInt(parts[0]);
    }

    private Types.DurationUnit parseDurationUnit(String durationStr) {
        String[] parts = durationStr.split(" ");
        String unit = parts[1].toUpperCase();
        return switch (unit) {
            case "S" -> Types.DurationUnit.SECOND;
            case "D" -> Types.DurationUnit.DAY;
            case "W" -> Types.DurationUnit.WEEK;
            case "M" -> Types.DurationUnit.MONTH;
            case "Y" -> Types.DurationUnit.YEAR;
            default -> Types.DurationUnit.DAY;
        };
    }

    private class HistoricalDataHandler implements ApiController.IHistoricalDataHandler {
        private final CompletableFuture<double[]> future;
        private final List<Bar> bars = new ArrayList<>();

        public HistoricalDataHandler(CompletableFuture<double[]> future) {
            this.future = future;
        }

        @Override
        public void historicalData(Bar bar) {
            bars.add(bar);
        }

        @Override
        public void historicalDataEnd() {
            if (bars.size() >= 2) {
                Bar lastFullBar = bars.get(bars.size() - 2);
                future.complete(new double[]{lastFullBar.high(), lastFullBar.low()});
            } else if (!bars.isEmpty()) {
                Bar lastBar = bars.get(bars.size() - 1);
                future.complete(new double[]{lastBar.high(), lastBar.low()});
            } else {
                future.completeExceptionally(new IllegalStateException("No historical bars received"));
            }
        }
    }


    public Map<String, Object> orderToDict(OrderClient order) {
        Map<String, Object> data = new HashMap<>();

        data.put("id", order.getId());
        data.put("orders_random_id", order.getOrdersRandomId());
        data.put("client_db_id", order.getClientDbId());
        data.put("client_name", order.getClientName());
        data.put("account_id", order.getAccountId());
        data.put("risk_multiplier", order.getRiskMultiplier());
        data.put("fund", order.getFund());
        data.put("max_stock", order.getMaxStock());
        data.put("rm_option", order.getRmOption());
        data.put("rm_stock", order.getRmStock());
        data.put("quantity", order.getQuantity());
        data.put("parent_id", order.getParentId());
        data.put("entry_price", order.getEntryPrice());
        data.put("entry_filled_price", order.getEntryFilledPrice());
        data.put("tp_filled_price", order.getTpFilledPrice());
        data.put("tp_price", order.getTpPrice());
        data.put("sl_price", order.getSlPrice());
        data.put("strike", order.getStrike());
        data.put("entry_id", order.getEntryId());
        data.put("tp_temp_id", order.getTpTempId());
        data.put("sl_temp_id", order.getSlTempId());
        data.put("tp_id", order.getTpId());
        data.put("sl_id", order.getSlId());
        data.put("entry_status", order.getEntryStatus());
        data.put("tp_status", order.getTpStatus());
        data.put("sl_status", order.getSlStatus());
        data.put("active", order.getActive());

        if (order.getCreatedAt() != null) {
            data.put("created_at", order.getCreatedAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } else {
            data.put("created_at", null);
        }

        data.put("symbol", order.getSymbol());
        data.put("exchange", order.getExchange());
        data.put("currency", order.getCurrency());
        data.put("maturitydate", order.getMaturityDate());
        data.put("trading_class", order.getTradingClass());
        data.put("call_put", order.getCallPut());
        data.put("action", order.getAction());
        data.put("security_type", order.getSecurityType());
        data.put("order_type", order.getOrderType());
        data.put("price", order.getPrice());
        data.put("error_message", order.getErrorMessage());

        return data;
    }

    private void sendOrderToApi(OrderClient order) {
        try {
            Token tokenRecord = DatabaseConfig.getToken();
            String authToken = tokenRecord != null ? tokenRecord.getToken() : "";
            ConnectionEntity conn = DatabaseConfig.getConnectionEntity();
            String connName = conn != null ? conn.getConnectionName() : "";

            log.debug("Sending order: {}", order);
            Map<String, Object> data = orderToDict(order);

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/exe_save_orders");
                String payload = gson.toJson(data);
                log.info("payload to send to API: {}", payload);
                post.setEntity(new StringEntity(payload));
                post.setHeader("Authorization", authToken + "_" + connName);
                post.setHeader("Content-Type", "application/json");

                try (CloseableHttpResponse response = client.execute(post)) {
                    String responseText = EntityUtils.toString(response.getEntity());
                    log.info("Order API response: {} for {}", responseText, order.getEntryId());

                    Map<String, Object> updateFields = new HashMap<>();
                    if (response.getStatusLine().getStatusCode() == 200) {
                        log.info("Order sent successfully to API: {}", order.getEntryId());
                    } else {
                        log.info("Failed to send order to API: {}", order.getEntryId());
                    }

                }
            }
        } catch (Exception e) {
            log.warn("Error sending order to API: {}", e.getMessage());
            log.error("Error sending order: {}", e.getMessage(), e);
        }
    }

    private void orderStatusLoop(int orderId, OrderStatus status, Decimal filled, Decimal remaining, double avgFillPrice,
                                 int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) {
        log.info("orderStatusLoop called: orderId={}, status={}, filled={}, remaining={}",
                orderId, status, filled, remaining);

        if (status == null) {
            log.warn("Status is null, exiting orderStatusLoop for orderId={}", orderId);
            return;
        }

        String statusStr = status.name();
        log.debug("Converted status to string: statusStr={}", statusStr);

        double filledPrice = "Filled".equals(statusStr) ? avgFillPrice : 0;
        log.debug("Calculated filledPrice: filledPrice={}", filledPrice);

        // Update openOrders
        synchronized (openOrders) {
            for (Map.Entry<Order, Contract> entry : openOrders) {
                if (entry.getKey().orderId() == orderId) {
                    Order updatedOrder = entry.getKey();
                    updatedOrder.totalQuantity(remaining != null ? remaining : updatedOrder.totalQuantity());
                    log.info("Updated open order in orderStatusLoop: orderId={}, status={}, remaining={}",
                            orderId, statusStr, remaining);
                    break;
                }
            }
        }

        try {
            log.info("Retrieving order clients for orderId={}", orderId);
            OrderClient client = DatabaseConfig.getOrderClientByParentId(String.valueOf(orderId));
            OrderClient tpClient = DatabaseConfig.getOrderClientByTpTempId(String.valueOf(orderId));
            OrderClient slClient = DatabaseConfig.getOrderClientBySlTempId(String.valueOf(orderId));

            log.debug("Retrieved clients:orderId={} client={}, tpClient={}, slClient={}", orderId,
                    client != null, tpClient != null, slClient != null);

            if (client != null) {

                log.info("Updating main client for orderId={}", orderId);
                Map<String, Object> updateFields = new HashMap<>();
                updateFields.put("entry_status", statusStr);
                updateFields.put("entry_filled_price", (float) filledPrice);
                updateFields.put("error_message", "Entry order " + statusStr);
                updateFields.put("entry_id", String.valueOf(permId));
                updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                Instant nowUtc = Instant.now();
                LocalDateTime utcDateTime = LocalDateTime.ofInstant(nowUtc, ZoneOffset.UTC);
                updateFields.put("created_at", utcDateTime);
                updateFields.put("remaining", remaining != null ? (float) remaining.longValue() : 0.0f);
                DatabaseConfig.updateOrderClient(client, updateFields);
                log.info("Main client update persisted for orderId={}", orderId);

                OrderClient client_new = DatabaseConfig.getOrderClientByParentId(String.valueOf(orderId));
                sendOrderToApi(client_new);
                log.info("Sending order to API for orderId={}", orderId);

            } else if (tpClient != null) {
                log.info("Updating take-profit client for orderId={}", orderId);
                Map<String, Object> updateFields = new HashMap<>();
                updateFields.put("tp_id", String.valueOf(permId));
                updateFields.put("tp_price", (float) filledPrice);
                updateFields.put("tp_status", statusStr);
                updateFields.put("tp_filled_price", (float) filledPrice);
                Instant nowUtc = Instant.now();
                LocalDateTime utcDateTime = LocalDateTime.ofInstant(nowUtc, ZoneOffset.UTC);
                updateFields.put("created_at", utcDateTime);
                updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                DatabaseConfig.updateOrderClient(tpClient, updateFields);
                log.info("TP client update persisted for orderId={}", orderId);

                OrderClient tpClient_new = DatabaseConfig.getOrderClientByTpTempId(String.valueOf(orderId));
                sendOrderToApi(tpClient_new);
                log.info("Sending order to API for orderId={}", orderId);

            } else if (slClient != null ) {
                log.info("Updating stop-loss client for orderId={}", orderId);
                Map<String, Object> updateFields = new HashMap<>();
                updateFields.put("sl_status", statusStr);
                updateFields.put("sl_price", (float) filledPrice);
                updateFields.put("sl_id", String.valueOf(permId));
                Instant nowUtc = Instant.now();
                LocalDateTime utcDateTime = LocalDateTime.ofInstant(nowUtc, ZoneOffset.UTC);
                updateFields.put("created_at", utcDateTime);
                updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                DatabaseConfig.updateOrderClient(slClient, updateFields);
                log.info("SL client update persisted for orderId={}", orderId);

                OrderClient slClient_new = DatabaseConfig.getOrderClientBySlTempId(String.valueOf(orderId));
                sendOrderToApi(slClient_new);
                log.info("Sending order to API for orderId={}", orderId);


            } else {
                log.warn("No valid client found or status is INACTIVE for orderId={}", orderId);
            }
        } catch (SQLException e) {
            log.error("Error updating order status for orderId={}: {}", orderId, e.getMessage(), e);
        }
    }

    private void errorFunc(int reqId, int errorCode, String errorString, Contract contract) {
        ErrorLog error = new ErrorLog();
        error.setReqId(String.valueOf(reqId));
        error.setErrorCode(String.valueOf(errorCode));
        error.setErrorString(errorString);
        error.setContract(contract != null ? contract.toString() : "");
        try {
            DatabaseConfig.saveErrorData(error);

            OrderClient client = DatabaseConfig.getOrderClientByParentId(String.valueOf(reqId));
            OrderClient tpClient = DatabaseConfig.getOrderClientByTpTempId(String.valueOf(reqId));
            OrderClient slClient = DatabaseConfig.getOrderClientBySlTempId(String.valueOf(reqId));

            if (!"Order Canceled - reason:".equals(errorString) && !errorString.contains("Warning")) {
                if (client != null) {
                    Map<String, Object> updateFields = new HashMap<>();
                    updateFields.put("error_message", errorString);
                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                    DatabaseConfig.updateOrderClient(client, updateFields);
                } else if (tpClient != null) {
                    Map<String, Object> updateFields = new HashMap<>();
                    updateFields.put("error_message", errorString);
                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                    DatabaseConfig.updateOrderClient(tpClient, updateFields);
                } else if (slClient != null) {
                    Map<String, Object> updateFields = new HashMap<>();
                    updateFields.put("error_message", errorString);
                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                    DatabaseConfig.updateOrderClient(slClient, updateFields);
                }
            }
        } catch (SQLException e) {
            log.error("Error saving error data: {}", e.getMessage());
        }
    }
}