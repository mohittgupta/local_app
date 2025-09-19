package com.pickmytrade.ibapp.bussinesslogic;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ib.client.*;
import com.pickmytrade.ibapp.db.DatabaseConfig;
import com.pickmytrade.ibapp.db.entities.OrderClient;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.pickmytrade.ibapp.config.Config.log;

public class PlaceOrderService {
    private TwsEngine twsEngine;
    private final ExecutorService executor = Executors.newFixedThreadPool(32);
    private final Gson gson = new Gson();
    private final Map<Integer, Contract> orderToContractMap = new HashMap<>();

    public PlaceOrderService(TwsEngine twsEngine) {
        this.twsEngine = twsEngine;
    }

    public synchronized void setTwsEngine(TwsEngine twsEngine) {
        if (twsEngine == null) {
            log.error("Attempted to set null TwsEngine");
            throw new IllegalArgumentException("TwsEngine cannot be null");
        }
        log.info("Updating TwsEngine in PlaceOrderService");
        this.twsEngine = twsEngine;
    }

    public CompletableFuture<Boolean> placeTrade(Map<String, Object> contracts) {
        log.info("Entering placeTrade with contracts: {}", contracts);
        return CompletableFuture.supplyAsync(() -> {
            log.info("Received data to place order: {}", contracts);
            String orderRandomId = (String) contracts.get("random_alert_key");
            Map<String, Object> orderJson = (Map<String, Object>) contracts.get("order_details");
            Map<String, Object> contractJson = (Map<String, Object>) contracts.get("contract_details");
            String account = (String) contracts.get("account");
            Double entryOrderPrice = null;
            String entryOrderId = null;
            String entryOrderStatus = null;
            Order stopLossOrder = null;

            double minTick = contracts.get("min_tick") != null && ((Number) contracts.get("min_tick")).doubleValue() != 0
                    ? ((Number) contracts.get("min_tick")).doubleValue() : 1;
            double breakEven = contracts.get("break_even") != null && ((Number) contracts.get("break_even")).doubleValue() != 0
                    ? ((Number) contracts.get("break_even")).doubleValue() : 0;
            boolean newBreakEvenOrder = contracts.get("new_break_even_order") != null
                    && (boolean) contracts.get("new_break_even_order");
            int lmtToMarketWait = contracts.get("lmt_to_market_wait") != null
                    && ((Number) contracts.get("lmt_to_market_wait")).intValue() != 0
                    ? ((Number) contracts.get("lmt_to_market_wait")).intValue() : 0;
            boolean reverseOrderClose = orderJson.get("reverse_order_close") != null
                    && (boolean) orderJson.get("reverse_order_close");
            boolean duplicatePositionAllow = orderJson.get("duplicate_position_allow") != null
                    && (boolean) orderJson.get("duplicate_position_allow");
            int con_id = orderJson.get("con_id") != null && ((Number) orderJson.get("con_id")).intValue() != 0
                    ? ((Number) orderJson.get("con_id")).intValue() : 0;

            Object quantityObj = orderJson.get("quantity");
            int quantity;
            if (quantityObj instanceof Number && ((Number) quantityObj).intValue() != 0) {
                quantity = ((Number) quantityObj).intValue();
            } else {
                log.error("Invalid quantity: must be a non-zero number, got: {}", quantityObj);
                throw new IllegalArgumentException("Quantity must be a non-zero number");
            }

            if ("NULL".equals(contractJson.get("maturityDate"))) {
                contractJson.put("maturityDate", "");
            }

            try {
                log.debug("Processing trade for orderRandomId: {}", orderRandomId);
                if (!twsEngine.isConnected()) {
                    log.error("TWS is not connected in PlaceOrderService");
                    throw new IllegalStateException("TWS is not connected");
                }
                // CHANGED: Use getAllPositions() instead of getPositions(account).join()
                List<Map<String, Object>> positions = twsEngine.getAllPositions();
                List<Map.Entry<Order, Contract>> openOrders = twsEngine.getAllOpenOrders();
                if ("CLOSE".equalsIgnoreCase((String) orderJson.get("action"))) {

                    for (Map<String, Object> pos : positions) {
                        if (!pos.get("account").equals(account)) {
                            continue;
                        }
                        log.info("Closing all positions for {} with order_random_id: {}", contractJson.get("symbol"), orderRandomId);
                        Contract contract = (Contract) pos.get("contract");
                        Decimal position = (Decimal) pos.get("position");
                        boolean pos_symbol_ck = String.valueOf(contract.symbol()).equals(String.valueOf(contractJson.get("symbol")));
                        boolean pos_sec_ck = Objects.equals(String.valueOf(contract.secType()), contractJson.get("inst_type").toString().toUpperCase());
                        boolean pos_last_date = String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"));
                        if (pos_symbol_ck && pos_sec_ck && pos_last_date) {
                            if (String.valueOf(contract.secType()).equals("OPT") || String.valueOf(contract.secType()).equals("FOP")) {
                                String contractRight = String.valueOf(contract.right());
                                String jsonRight = (String) contractJson.get("right");
                                if (contractRight == null || jsonRight == null ||
                                        !contractRight.substring(0, 1).equalsIgnoreCase(jsonRight.substring(0, 1))) {
                                    continue;
                                }
                            }
                            if (position.longValue() != 0) {
                                String buySell = position.longValue() < 0 ? "BUY" : "SELL";
                                Object closeOrderObj = twsEngine.createOrder("MKT", buySell, (int) Math.abs(position.longValue()),
                                        null, null, null, null, null, null, null, null, null, account, false, false, null, null, null );
                                if (closeOrderObj instanceof Order) {
                                    log.info("Closing position for contract: {}", contract);
                                    Order closeOrder = (Order) closeOrderObj;
                                    contract.exchange(contractJson.get("exchange").toString().toUpperCase());
                                    TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(contract, closeOrder);
                                    Order executedOrder = result.getOrder();
                                    orderToContractMap.put(executedOrder.orderId(), contract);
//                                    Thread.sleep(100);
                                    if (DatabaseConfig.getErrorData(contract.toString()) != null) {
                                        continue;


                                    }
                                }
                            }
                        }
                    }

                    for (Map.Entry<Order, Contract> order_contract : openOrders) {

                        Order order = order_contract.getKey();
                        Contract contract = order_contract.getValue();
                        if (!String.valueOf(order.account()).equals(account)) continue;
                        if (contract == null) continue;
                        OrderClient orderClient = DatabaseConfig.getOrderClientByParentId(String.valueOf(order.orderId()));
                        String orderStatus = orderClient != null ? orderClient.getEntryStatus() : "Unknown";
                        if (String.valueOf(contract.symbol()).equals(contractJson.get("symbol")) &&
                                String.valueOf(contract.secType()).equals(contractJson.get("inst_type").toString().toUpperCase()) &&
                                String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"))) {
                            if (Arrays.asList("ApiPending", "PendingSubmit", "PreSubmitted", "Submitted", "Unknown").contains(orderStatus)) {
                                if (String.valueOf(contract.secType()).equals("OPT") || String.valueOf(contract.secType()).equals("FOP")) {
                                    String contractRight = String.valueOf(contract.right());
                                    String jsonRight = (String) contractJson.get("right");
                                    if (contractRight == null || jsonRight == null ||
                                            !contractRight.substring(0, 1).equalsIgnoreCase(jsonRight.substring(0, 1))) {
                                        continue;
                                    }
                                }
                                log.info("cancelling order: {} for contract: {}", order.orderId(), contract);
                                twsEngine.cancelTrade(order);
                            }
                        }
                    }
                    saveOrderToDatabase(orderJson, contracts, orderRandomId, null, null, "", "", contractJson, "Position closed successfully.", 0);
                    return true;
                }

                if (!duplicatePositionAllow) {
                    for (Map<String, Object> pos : positions) {
                        if (!pos.get("account").equals(account)) {
                            continue;
                        }
                        Contract contract = (Contract) pos.get("contract");
                        Decimal position = (Decimal) pos.get("position");
                        boolean pos_symbol_ck = String.valueOf(contract.symbol()).equals(String.valueOf(contractJson.get("symbol")));
                        boolean pos_sec_ck = Objects.equals(String.valueOf(contract.secType()), contractJson.get("inst_type").toString().toUpperCase());
                        boolean pos_last_date = String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"));
                        boolean pos_action_data = ("BUY".equals(orderJson.get("action")) && position.longValue() > 0);
                        boolean pos_action_ck = ("SELL".equals(orderJson.get("action")) && position.longValue() < 0);
                        if (pos_symbol_ck && pos_sec_ck && pos_last_date) {
                            if (pos_action_data || pos_action_ck) {
                                if (String.valueOf(contract.secType()).equals("OPT") || String.valueOf(contract.secType()).equals("FOP")) {
                                    String contractRight = String.valueOf(contract.right());
                                    String jsonRight = (String) contractJson.get("right");
                                    if (contractRight == null || jsonRight == null ||
                                            !contractRight.substring(0, 1).equalsIgnoreCase(jsonRight.substring(0, 1))) {
                                        continue;
                                    }
                                }
                                saveOrderToDatabase(orderJson, contracts, orderRandomId, null, null, "", "", contractJson, "Can not Send, Duplicate Position Found.",0);
                                return false;
                            }
                        }
                    }
                }

                if (contracts.get("strike_start") != null &&
                        ((Number) contracts.get("strike_start")).doubleValue() != 0) {
                    double strike = getBestStrikeForOption(contractJson,
                            ((Number) contracts.get("strike_start")).doubleValue(),
                            ((Number) contracts.get("strike_end")).doubleValue(),
                            ((Number) contracts.get("strike_interval")).doubleValue(),
                            ((Number) contracts.get("premium_start")).doubleValue(),
                            ((Number) contracts.get("premium_end")).doubleValue());
                    if (strike != 0) contractJson.put("strike", strike);
                }


                Double strike = null;
                Object strikeObj = contractJson.get("strike");
                String instType = (String) contractJson.get("inst_type");

// Validate strike only for OPT or FOP
                if ("OPT".equalsIgnoreCase(instType) || "FOP".equalsIgnoreCase(instType)) {
                    if (strikeObj instanceof Number) {
                        double value = ((Number) strikeObj).doubleValue();
                        if (value != 0) {
                            strike = value;
                        } else {
                            saveOrderToDatabase(orderJson, contracts, orderRandomId, null, null, "", "", contractJson, "Please pass valid Strike", 0);
                            return false;
                        }
                    } else {
                        saveOrderToDatabase(orderJson, contracts, orderRandomId, null, null, "", "", contractJson, "Strike must be a number", 0);
                        return false;
                    }
                }

                double order_strike = 0;


                if (("OPT".equalsIgnoreCase(instType) || "FOP".equalsIgnoreCase(instType)) && strike != null) {
                    order_strike = twsEngine.getClosestStrike(con_id, strike);
                } else if (strike != null) {
                    order_strike = strike;
                }


                if (order_strike == 0 || Double.isNaN(order_strike)) {
                    order_strike = strike != null ? strike : 0;
                }


                Contract ibContract = twsEngine.createContract(
                    (String) contractJson.get("inst_type"),
                    (String) contractJson.get("symbol"),
                    (String) contractJson.get("exchange"),
                    (String) contractJson.get("currency"),
                    order_strike,
                    (String) contractJson.get("right"),
                    null,
                    (String) contractJson.get("maturityDate"),
                    (String) contractJson.get("trading_class"));

                if (reverseOrderClose) {
                    String oppositeAction = "BUY".equals(orderJson.get("action")) ? "SELL" : "BUY";
                    for (Map<String, Object> pos : positions) {
                        if (!pos.get("account").equals(account)) {
                            continue;
                        }
                        log.info("Closing positions for {} with order_random_id: {}", contractJson.get("symbol"), orderRandomId);
                        Contract contract = (Contract) pos.get("contract");
                        Decimal position = (Decimal) pos.get("position");
                        boolean pos_symbol_ck = String.valueOf(contract.symbol()).equals(String.valueOf(contractJson.get("symbol")));
                        boolean pos_sec_ck = Objects.equals(String.valueOf(contract.secType()), contractJson.get("inst_type").toString().toUpperCase());
                        boolean pos_last_date = String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"));

                        boolean shouldClosePosition = false;

                        if (String.valueOf(contract.secType()).equals("OPT") || String.valueOf(contract.secType()).equals("FOP")) {
                            String contractRight = String.valueOf(contract.right());
                            String jsonRight = (String) contractJson.get("right");
                            if (contractRight != null && jsonRight != null) {
                                boolean isOppositeRight = ("C".equalsIgnoreCase(jsonRight.substring(0, 1)) && "P".equalsIgnoreCase(contractRight.substring(0, 1))) ||
                                        ("P".equalsIgnoreCase(jsonRight.substring(0, 1)) && "C".equalsIgnoreCase(contractRight.substring(0, 1)));
                                boolean isSameRightOppositeAction = jsonRight.substring(0, 1).equalsIgnoreCase(contractRight.substring(0, 1)) &&
                                        (("BUY".equals(orderJson.get("action")) && position.longValue() < 0) ||
                                                ("SELL".equals(orderJson.get("action")) && position.longValue() > 0));

                                shouldClosePosition = pos_symbol_ck && pos_sec_ck  && (isOppositeRight || (isSameRightOppositeAction && pos_last_date));
                            }
                        } else {
                            // FUT and STK: Original logic
                            boolean pos_action_data = ("BUY".equals(orderJson.get("action")) && position.longValue() < 0);
                            boolean pos_action_ck = ("SELL".equals(orderJson.get("action")) && position.longValue() > 0);
                            boolean pos_action_reverse = ("BUY".equals(oppositeAction) && position.longValue() > 0) ||
                                    ("SELL".equals(oppositeAction) && position.longValue() < 0);
                            shouldClosePosition = pos_symbol_ck && pos_sec_ck && pos_last_date && (pos_action_data || pos_action_ck) && pos_action_reverse;
                        }

                        if (shouldClosePosition) {
                            String close_action = position.longValue() > 0 ? "SELL" : "BUY"; // Using the fixed close_action logic
                            Object closeOrderObj = twsEngine.createOrder("MKT", close_action,
                                    (int) Math.abs(position.longValue()), null, null, null, null, null, null, null, null, null,
                                    account, false, false, null, null, null);
                            if (closeOrderObj instanceof Order) {
                                Order closeOrder = (Order) closeOrderObj;
                                contract.exchange(contractJson.get("exchange").toString().toUpperCase());
                                TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(contract, closeOrder);
                                log.info("Closing position for contract: {}", contract);
                                // Thread.sleep(100); // Commented out as in original code
                            }
                        }
                    }

                    for (Map.Entry<Order, Contract> order_contract : openOrders) {
                        Order order = order_contract.getKey();
                        Contract contract = order_contract.getValue();
                        if (!String.valueOf(order.account()).equals(account)) continue;
                        String tag = order.orderRef();
                        OrderClient orderClient = DatabaseConfig.getOrderClientByParentId(String.valueOf(order.orderId()));
                        String orderStatus = orderClient != null ? orderClient.getEntryStatus() : "Unknown";

                        boolean shouldCancelOrder = false;
                        if (contract != null && String.valueOf(contract.symbol()).equals(contractJson.get("symbol")) &&
                                String.valueOf(contract.secType()).equals(contractJson.get("inst_type").toString().toUpperCase())) {
                            if (String.valueOf(contract.secType()).equals("OPT") || String.valueOf(contract.secType()).equals("FOP")) {
                                String contractRight = String.valueOf(contract.right());
                                String jsonRight = (String) contractJson.get("right");
                                if (contractRight != null && jsonRight != null) {
                                    boolean isOppositeRight = ("C".equalsIgnoreCase(jsonRight.substring(0, 1)) && "P".equalsIgnoreCase(contractRight.substring(0, 1))) ||
                                            ("P".equalsIgnoreCase(jsonRight.substring(0, 1)) && "C".equalsIgnoreCase(contractRight.substring(0, 1)));
                                    boolean isSameRightOppositeAction = jsonRight.substring(0, 1).equalsIgnoreCase(contractRight.substring(0, 1)) &&
                                            ((String.valueOf(order.action()).equals(orderJson.get("action")) && Arrays.asList("TP", "SL").contains(tag)) ||
                                                    (!String.valueOf(order.action()).equals(orderJson.get("action")) && "ENTRY".equals(tag)));
                                    boolean or_last_date = String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"));
                                    shouldCancelOrder = (isOppositeRight || (isSameRightOppositeAction && or_last_date)) &&
                                            Arrays.asList("ApiPending", "PendingSubmit", "PreSubmitted", "Submitted", "Unknown").contains(orderStatus);
                                }
                            } else {
                                if
                                (String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"))) {
                                    // FUT and STK: Original logic
                                    shouldCancelOrder = ((String.valueOf(order.action()).equals(orderJson.get("action")) && Arrays.asList("TP", "SL").contains(tag)) ||
                                            (!String.valueOf(order.action()).equals(orderJson.get("action")) && "ENTRY".equals(tag))) &&
                                            Arrays.asList("ApiPending", "PendingSubmit", "PreSubmitted", "Submitted", "Unknown").contains(orderStatus);
                                }
                            }
                        }

                        if (shouldCancelOrder) {
                            log.info("Cancelling order: {} for contract: {}", order.orderId(), contract);
                            twsEngine.cancelTrade(order);
                        }
                    }

                }
                log.info("creating order for contract: {}", ibContract);
                Object tradeOrderObj = twsEngine.createOrder(
                        (String) orderJson.get("trade_type"),
                        (String) orderJson.get("action"),
                        quantity,
                        orderJson.get("stop_price") != null && ((Double) orderJson.get("stop_price")) != 0
                                ? (Double) orderJson.get("stop_price") : null,
                        orderJson.get("limit_price") != null && ((Double) orderJson.get("limit_price")) != 0
                                ? (Double) orderJson.get("limit_price") : null,
                        orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                                ? (Double) orderJson.get("tp_price") : null,
                        orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                                ? (Double) orderJson.get("sl_price") : null,
                        orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                                ? (Double) orderJson.get("sl_dollar") : null,
                        orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                                ? (Double) orderJson.get("tp_dollar") : null,
                        orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                                ? (Double) orderJson.get("sl_percentage") : null,
                        orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                                ? (Double) orderJson.get("tp_percentage") : null,
                        orderJson.get("trailing_amount") != null && ((Double) orderJson.get("trailing_amount")) != 0
                                ? (Double) orderJson.get("trailing_amount") : null,
                        account,
                        reverseOrderClose,
                        duplicatePositionAllow,
                        orderJson.get("entry_lmt_price_offset") != null && ((Double) orderJson.get("entry_lmt_price_offset")) != 0
                                ? (Double) orderJson.get("entry_lmt_price_offset") : null,
                        orderJson.get("entry_trailing_amount") != null && ((Double) orderJson.get("entry_trailing_amount")) != 0
                                ? (Double) orderJson.get("entry_trailing_amount") : null,
                        orderJson.get("sl_lmt_price_offset") != null && ((Double) orderJson.get("sl_lmt_price_offset")) != 0
                                ? (Double) orderJson.get("sl_lmt_price_offset") : null
                        );
                log.info("Trade order object created: {}", tradeOrderObj);

                String ocaGroupId = generateRandomKey(7) + generateRandomKey(8);
                if (tradeOrderObj instanceof List) {
                    List<Order> tradeOrders = new ArrayList<>((List<Order>) tradeOrderObj);

                    String orderId = null;
                    String tpTempId = null;
                    String slTempId = null;
                    double tp = 0;
                    double sl = 0;
                    String entryOrderFilled = "";

                    for (int i = 0; i < tradeOrders.size(); i++) {
                        log.info("Processing order {} of {}: {}", i + 1, tradeOrders.size(), tradeOrders.get(i));
                        Order order = tradeOrders.get(i);
                        if (order == null) continue;
                        order.outsideRth(true);
                        order.account(account);
                        if (i == 0) {
                            order.orderRef("ENTRY");
                            TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, order);
                            log.info("order result: {}", result);
                            Order executedOrder = result.getOrder();
                            log.info("executed order: {}", executedOrder);
                            orderToContractMap.put(executedOrder.orderId(), ibContract);
                            long startTime = System.currentTimeMillis();

                            if (executedOrder != null) {
                                log.info("executed order id: {}", executedOrder.orderId());
                                orderId = String.valueOf(executedOrder.orderId());
                                log.info("executed order id: {}", executedOrder.orderId());
                                entryOrderId = String.valueOf(executedOrder.permId());
                                log.info("executed order id: {}", executedOrder.permId());
                                OrderClient orderClient = DatabaseConfig.getOrderClientByParentId(orderId);
                                entryOrderStatus = orderClient != null ? orderClient.getEntryStatus() : "Unknown";
                                entryOrderPrice = orderClient != null && orderClient.getEntryFilledPrice() != null
                                        ? (double) orderClient.getEntryFilledPrice() : 0.0;
                                saveOrderToDatabase(orderJson, contracts, orderRandomId, entryOrderPrice, orderId,
                                        entryOrderId, entryOrderStatus, contractJson, null, quantity);

                                if (("LMT".equals(orderJson.get("trade_type")) || "TRAIL LIMIT".equals(orderJson.get("trade_type"))) && lmtToMarketWait > 0) {
                                    int maxWait = Math.max(lmtToMarketWait * 1000, 1000);
                                    while (System.currentTimeMillis() - startTime < maxWait) {
                                        log.info("Waiting for {} ms before placing Market order", maxWait);
                                        OrderClient entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                                        entryOrderFilled = entryOrderDbData != null ? entryOrderDbData.getEntryStatus() : "Unknown";
                                        if ("Filled".equals(entryOrderFilled)) {
                                            entryOrderPrice = entryOrderDbData.getEntryFilledPrice() != null
                                                    ? (double) entryOrderDbData.getEntryFilledPrice() : 0.0;
                                            log.info("Limit order filled!");
                                            break;
                                        }
                                        Thread.sleep(50);
                                    }
                                    if (!"Filled".equals(entryOrderFilled)) {
                                        log.info("Limit order not filled in time, placing market order.");
                                        twsEngine.cancelTrade(executedOrder);
//                                        Thread.sleep(100);
                                        OrderClient entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                                        int remaining = entryOrderDbData != null ? entryOrderDbData.getRemaining().intValue() : quantity;
                                        log.info("Placing market order for remaining quantity: {}", remaining);
                                        Order marketOrder = new Order();
                                        marketOrder.action((String) orderJson.get("action"));
                                        marketOrder.orderType("MKT");
                                        marketOrder.totalQuantity(Decimal.get(remaining));
                                        marketOrder.account(account);
                                        marketOrder.orderRef("ENTRY");
                                        TwsEngine.OrderExecutionResult marketResult = twsEngine.executeOrder(ibContract, marketOrder);
                                        executedOrder = marketResult.getOrder();
                                        orderToContractMap.put(executedOrder.orderId(), ibContract);
                                        if (executedOrder != null) {

                                            OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                            orderId = String.valueOf(executedOrder.orderId());
                                            entryOrderId = String.valueOf(executedOrder.permId());
                                            OrderClient updatedClient = DatabaseConfig.getOrderClientByParentId(orderId);
                                            entryOrderStatus = updatedClient != null ? updatedClient.getEntryStatus() : "Unknown";
                                            entryOrderPrice = updatedClient != null && updatedClient.getEntryFilledPrice() != null
                                                    ? (double) updatedClient.getEntryFilledPrice() : 0.0;
                                            if (clients != null) {
                                                Map<String, Object> updateFields = new HashMap<>();
                                                updateFields.put("parent_id", orderId);
                                                updateFields.put("entry_id", entryOrderId);
                                                updateFields.put("entry_status", entryOrderStatus);
                                                updateFields.put("order_type", "MKT");
                                                updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                                DatabaseConfig.updateOrderClient(clients, updateFields);
                                            }
                                        }
                                    }
                                }

                                while (!"Filled".equals(entryOrderFilled)) {

                                    Thread.sleep(50);
                                    OrderClient entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                                    entryOrderFilled = entryOrderDbData.getEntryStatus();
                                    entryOrderPrice = entryOrderDbData != null && entryOrderDbData.getEntryFilledPrice() != null
                                            ? (double) entryOrderDbData.getEntryFilledPrice() : 0.0;
                                    if ("Cancelled".equals(entryOrderFilled)) break;
                                }
                            }
                        } else if (i == 1 && order != null && "Filled".equals(entryOrderFilled)) {
                            while (tp == 0) {

                                double[] tpSl = getTpSlPrice((String) contractJson.get("inst_type"), entryOrderPrice,
                                        (String) orderJson.get("action"),
                                        orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                                                ? (Double) orderJson.get("tp_price") : null,
                                        orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                                                ? (Double) orderJson.get("sl_price") : null,
                                        orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                                                ? (Double) orderJson.get("sl_dollar") : null,
                                        orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                                                ? (Double) orderJson.get("tp_dollar") : null,
                                        orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                                                ? (Double) orderJson.get("sl_percentage") : null,
                                        orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                                                ? (Double) orderJson.get("tp_percentage") : null,
                                        minTick);
                                tp = tpSl[0];
                                sl = tpSl[1];
                            }
                            order.lmtPrice(tp);
                            order.ocaGroup(ocaGroupId);
                            order.ocaType(1);
                            order.orderRef("TP");
                            TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, order);
                            Order executedOrder = result.getOrder();
                            log.info("order result: {}", result);
                            orderToContractMap.put(executedOrder.orderId(), ibContract);
                            if (executedOrder != null) {
//                                Thread.sleep(100);
                                tpTempId = String.valueOf(executedOrder.orderId());
                                OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                if (clients != null) {
                                    Map<String, Object> updateFields = new HashMap<>();
                                    updateFields.put("tp_temp_id", tpTempId);
                                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                    DatabaseConfig.updateOrderClient(clients, updateFields);
                                }
                            }
                        } else if (i == 2 && order != null && "Filled".equals(entryOrderFilled)) {
                            OrderType ordertype = order.orderType();
                            double[] tpSl = getTpSlPrice((String) contractJson.get("inst_type"), entryOrderPrice,
                                    (String) orderJson.get("action"),
                                    orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                                            ? (Double) orderJson.get("tp_price") : null,
                                    orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                                            ? (Double) orderJson.get("sl_price") : null,
                                    orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                                            ? (Double) orderJson.get("sl_dollar") : null,
                                    orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                                            ? (Double) orderJson.get("tp_dollar") : null,
                                    orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                                            ? (Double) orderJson.get("sl_percentage") : null,
                                    orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                                            ? (Double) orderJson.get("tp_percentage") : null,
                                    minTick);
                            tp = tpSl[0];
                            sl = tpSl[1];

                            if ("STP".equals(ordertype.name())) {

                                order.auxPrice(sl);
                            } else if ("TRAIL LIMIT".equals(order.orderType())) {
                                order.trailStopPrice(sl);
                            }
                            order.ocaGroup(ocaGroupId);
                            order.ocaType(1);
//                            order.tif("GTC");
                            order.orderRef("SL");
                            TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, order);
                            Order executedOrder = result.getOrder();
                            log.info("order result: {}", result);
                            orderToContractMap.put(executedOrder.orderId(), ibContract);
                            if (executedOrder != null) {
                                stopLossOrder = executedOrder;
//                                Thread.sleep(100);
                                slTempId = String.valueOf(executedOrder.orderId());
                                OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                if (clients != null) {
                                    Map<String, Object> updateFields = new HashMap<>();
                                    updateFields.put("sl_temp_id", slTempId);
                                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                    DatabaseConfig.updateOrderClient(clients, updateFields);
                                }
                            }
                        }
                    }

                    if (breakEven != 0 && "Filled".equals(entryOrderFilled)) {
                        monitorAndUpdateStopLoss(entryOrderPrice, stopLossOrder, ibContract, orderJson, breakEven, orderId, newBreakEvenOrder, contracts);
                    }
                } else {
                    log.info("Processing single order for contract: {}", ibContract);
                    Order tradeOrder = (Order) tradeOrderObj;
                    tradeOrder.outsideRth(true);
                    tradeOrder.account(account);
                    tradeOrder.orderRef("ENTRY");
                    TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, tradeOrder);
                    Order executedOrder = result.getOrder();
                    log.info("Order execution result: {}", result);
                    System.out.println(executedOrder.orderId());
                    orderToContractMap.put(executedOrder.orderId(), ibContract);
                    long startTime = System.currentTimeMillis();
                    String orderId = String.valueOf(executedOrder.orderId());
                    entryOrderId = String.valueOf(executedOrder.permId());
                    OrderClient orderClient = DatabaseConfig.getOrderClientByParentId(orderId);
                    entryOrderStatus = orderClient != null ? orderClient.getEntryStatus() : "Unknown";
                    entryOrderPrice = orderClient != null && orderClient.getEntryFilledPrice() != null
                            ? (double) orderClient.getEntryFilledPrice() : 0.0;
                    saveOrderToDatabase(orderJson, contracts, orderRandomId, entryOrderPrice, orderId,
                            entryOrderId, entryOrderStatus, contractJson, null, quantity);

                    if (("LMT".equals(orderJson.get("trade_type")) || "TRAIL LIMIT".equals(orderJson.get("trade_type")))&& lmtToMarketWait > 0) {
                        String entryOrderFilled = "";
                        int maxWait = Math.max(lmtToMarketWait * 1000, 1000);
                        while (System.currentTimeMillis() - startTime < maxWait) {
                            log.info("Waiting for {} ms before placing Market order", maxWait);
                            OrderClient entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                            entryOrderFilled = entryOrderDbData != null ? entryOrderDbData.getEntryStatus() : "Unknown";
                            if ("Filled".equals(entryOrderFilled)) {
                                entryOrderPrice = entryOrderDbData.getEntryFilledPrice() != null
                                        ? (double) entryOrderDbData.getEntryFilledPrice() : 0.0;
                                log.info("Limit order filled!");
                                break;
                            }
                            Thread.sleep(50);
                        }
                        if (!"Filled".equals(entryOrderFilled)) {
                            log.info("Limit order not filled in time, placing market order.");
                            twsEngine.cancelTrade(executedOrder);
//                            Thread.sleep(100);
                            OrderClient entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                            int remaining = entryOrderDbData != null ? entryOrderDbData.getRemaining().intValue() : quantity;
                            Order marketOrder = new Order();
                            marketOrder.action((String) orderJson.get("action"));
                            marketOrder.orderType("MKT");
                            marketOrder.totalQuantity(Decimal.get(remaining));
                            marketOrder.account(account);
                            marketOrder.orderRef("ENTRY");
                            TwsEngine.OrderExecutionResult marketResult = twsEngine.executeOrder(ibContract, marketOrder);
                            executedOrder = marketResult.getOrder();
                            orderToContractMap.put(executedOrder.orderId(), ibContract);
                            if (executedOrder != null) {
//                                Thread.sleep(100);
                                OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                orderId = String.valueOf(executedOrder.orderId());
                                entryOrderId = String.valueOf(executedOrder.permId());
                                OrderClient updatedClient = DatabaseConfig.getOrderClientByParentId(orderId);
                                entryOrderStatus = updatedClient != null ? updatedClient.getEntryStatus() : "Unknown";
                                entryOrderPrice = updatedClient != null && updatedClient.getEntryFilledPrice() != null
                                        ? (double) updatedClient.getEntryFilledPrice() : 0.0;
                                if (clients != null) {
                                    Map<String, Object> updateFields = new HashMap<>();
                                    updateFields.put("parent_id", orderId);
                                    updateFields.put("entry_id", entryOrderId);
                                    updateFields.put("entry_status", entryOrderStatus);
                                    updateFields.put("order_type", "MKT");
                                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                    DatabaseConfig.updateOrderClient(clients, updateFields);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                String error = e.getMessage() != null ? e.getMessage().replace("'", "") : "Unknown error";
                log.error("Error placing trade: {}", error, e);
                saveOrderToDatabase(orderJson, contracts, orderRandomId, entryOrderPrice, null, "", "", contractJson, error, 0);
                return false;
            }
            log.info("placeTrade completed for orderRandomId: {}", orderRandomId);
            return true;
        }, executor);
    }

    public void handleOrderRecovery(String orderRandomId, String account, Map<String, Object> contractJson, Map<String, Object> orderJson, double minTick) {
        
        log.info("Recovering order with order_random_id: {}", orderRandomId);
        OrderClient entryOrderDbData = null;
        try {
            entryOrderDbData = DatabaseConfig.getOrderClientByOrderRandomId(orderRandomId);
        } catch (SQLException e) {
            log.error("Error retrieving order: {}", e.getMessage());
            return;
        }
        if (entryOrderDbData == null) {
            log.error("No order data found for order_random_id: {}", orderRandomId);
            return;
        }

        String entryOrderFilled = entryOrderDbData.getEntryStatus();
        double entryOrderPrice = entryOrderDbData.getEntryFilledPrice() != null ? entryOrderDbData.getEntryFilledPrice() : 0;
        double tp = 0;
        double sl = 0;
        String orderId = entryOrderDbData.getParentId();
        String temp_tp_id = entryOrderDbData.getTpTempId();
        String temp_sl_id = entryOrderDbData.getSlTempId();
        boolean positionOpened = false;

        if (!"Cancelled".equals(entryOrderFilled)) {
            log.info("Entry order already filled for {} with order_random_id: {}", contractJson.get("symbol"), orderRandomId);
            Object quantityObj = orderJson.get("quantity");
            int quantity = quantityObj instanceof Number && ((Number) quantityObj).intValue() != 0
                    ? ((Number) quantityObj).intValue() : 1;
            Object tradeOrderObj = twsEngine.createOrder(
                    (String) orderJson.get("trade_type"),
                    (String) orderJson.get("action"),
                    quantity,
                    orderJson.get("stop_price") != null && ((Double) orderJson.get("stop_price")) != 0
                            ? (Double) orderJson.get("stop_price") : null,
                    orderJson.get("limit_price") != null && ((Double) orderJson.get("limit_price")) != 0
                            ? (Double) orderJson.get("limit_price") : null,
                    orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                            ? (Double) orderJson.get("tp_price") : null,
                    orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                            ? (Double) orderJson.get("sl_price") : null,
                    orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                            ? (Double) orderJson.get("sl_dollar") : null,
                    orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                            ? (Double) orderJson.get("tp_dollar") : null,
                    orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                            ? (Double) orderJson.get("sl_percentage") : null,
                    orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                            ? (Double) orderJson.get("tp_percentage") : null,
                    orderJson.get("trailing_amount") != null && ((Double) orderJson.get("trailing_amount")) != 0
                            ? (Double) orderJson.get("trailing_amount") : null,
                    account,
                    false,
                    false,
                    orderJson.get("entry_lmt_price_offset") != null && ((Double) orderJson.get("entry_lmt_price_offset")) != 0
                            ? (Double) orderJson.get("entry_lmt_price_offset") : null,
                    orderJson.get("entry_trailing_amount") != null && ((Double) orderJson.get("entry_trailing_amount")) != 0
                            ? (Double) orderJson.get("entry_trailing_amount") : null,
                    orderJson.get("sl_lmt_price_offset") != null && ((Double) orderJson.get("sl_lmt_price_offset")) != 0
                            ? (Double) orderJson.get("sl_lmt_price_offset") : null
            );

            Contract ibContract = twsEngine.createContract(
                    (String) contractJson.get("inst_type"),
                    (String) contractJson.get("symbol"),
                    (String) contractJson.get("exchange"),
                    (String) contractJson.get("currency"),
                    contractJson.get("strike") != null && ((Number) contractJson.get("strike")).doubleValue() != 0
                            ? ((Number) contractJson.get("strike")).doubleValue() : null,
                    (String) contractJson.get("right"),
                    null,
                    (String) contractJson.get("maturityDate"),
                    (String) contractJson.get("trading_class"));

            if (tradeOrderObj instanceof List) {
                List<Order> tradeOrders = new ArrayList<>((List<Order>) tradeOrderObj);
                String ocaGroupId = generateRandomKey(15);

                if ("Filled".equals(entryOrderFilled)) {
                    // CHANGED: Use getAllPositions() instead of getPositions(account).join()
                    List<Map<String, Object>> positions = twsEngine.getAllPositions();
                    for (Map<String, Object> pos : positions) {
                        Contract contract = (Contract) pos.get("contract");
                        Decimal position = (Decimal) pos.get("position");
                        boolean pos_symbol_ck = String.valueOf(contract.symbol()).equals(String.valueOf(contractJson.get("symbol")));
                        boolean pos_sec_ck = Objects.equals(String.valueOf(contract.secType()), contractJson.get("inst_type").toString().toUpperCase());
                        boolean pos_last_date = String.valueOf(contract.lastTradeDateOrContractMonth()).equals(contractJson.get("maturityDate"));

                        if (pos_symbol_ck && pos_sec_ck && pos_last_date && quantity >= position.longValue()) {
                            positionOpened = true;
                            break;
                        }
                    }
                }

                for (int i = 0; i < tradeOrders.size(); i++) {
                    Order order = tradeOrders.get(i);
                    if (order == null) continue;
                    order.outsideRth(true);
                    order.account(account);
                    if (i == 0) continue;

                    while (!"Filled".equals(entryOrderFilled)) {
                        
                        try {

                            entryOrderDbData = DatabaseConfig.getOrderClientByParentId(orderId);
                            entryOrderFilled = entryOrderDbData != null ? entryOrderDbData.getEntryStatus() : "Unknown";
                            entryOrderPrice = entryOrderDbData != null && entryOrderDbData.getEntryFilledPrice() != null
                                    ? entryOrderDbData.getEntryFilledPrice() : 0;
                            if ("Cancelled".equals(entryOrderFilled)) break;
                        } catch (SQLException e) {
                            log.error("Error retrieving order: {}", e.getMessage());
                        }
                    }

                    if ("Filled".equals(entryOrderFilled)) positionOpened = true;

                    if (i == 1 && order != null && "Filled".equals(entryOrderFilled) ) {
                        if (temp_tp_id != null && !temp_tp_id.isEmpty()) {
                            log.info("Take-profit order already exists for order_random_id: {}, tp_temp_id: {}. Skipping TP order placement.", orderRandomId, temp_tp_id);
                            continue;
                        }
                        while (tp == 0) {
                            
                            double[] tpSl = getTpSlPrice((String) contractJson.get("inst_type"), entryOrderPrice,
                                    (String) orderJson.get("action"),
                                    orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                                            ? (Double) orderJson.get("tp_price") : null,
                                    orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                                            ? (Double) orderJson.get("sl_price") : null,
                                    orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                                            ? (Double) orderJson.get("sl_dollar") : null,
                                    orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                                            ? (Double) orderJson.get("tp_dollar") : null,
                                    orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                                            ? (Double) orderJson.get("sl_percentage") : null,
                                    orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                                            ? (Double) orderJson.get("tp_percentage") : null,
                                    minTick);
                            tp = tpSl[0];
                            sl = tpSl[1];
                        }
                        order.lmtPrice(tp);
                        order.ocaGroup(ocaGroupId);
                        order.ocaType(1);
                        order.orderRef("TP");
                        TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, order);
                        Order executedOrder = result.getOrder();
                        orderToContractMap.put(executedOrder.orderId(), ibContract);
                        if (executedOrder != null) {
                            try {
//                                Thread.sleep(1000);
                                OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                if (clients != null) {
                                    Map<String, Object> updateFields = new HashMap<>();
                                    updateFields.put("tp_temp_id", String.valueOf(executedOrder.orderId()));
                                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                    DatabaseConfig.updateOrderClient(clients, updateFields);
                                }
                            } catch (SQLException e) {
                                log.error("Error updating TP order: {}", e.getMessage());
                            }
                        }
                    } else if (i == 2 && order != null && positionOpened && "Filled".equals(entryOrderFilled)) {
                        if (temp_sl_id != null && !temp_sl_id.isEmpty()) {
                            log.info("Stop-loss order already exists for order_random_id: {}, sl_temp_id: {}. Skipping SL order placement.", orderRandomId, temp_sl_id);
                            continue;
                        }
                        OrderType ordertype = order.orderType();
                        if ("STP".equals(ordertype.name())) {
                            while (sl == 0) {
                                
                                double[] tpSl = getTpSlPrice((String) contractJson.get("inst_type"), entryOrderPrice,
                                        (String) orderJson.get("action"),
                                        orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                                                ? (Double) orderJson.get("tp_price") : null,
                                        orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                                                ? (Double) orderJson.get("sl_price") : null,
                                        orderJson.get("sl_dollar") != null && ((Double) orderJson.get("sl_dollar")) != 0
                                                ? (Double) orderJson.get("sl_dollar") : null,
                                        orderJson.get("tp_dollar") != null && ((Double) orderJson.get("tp_dollar")) != 0
                                                ? (Double) orderJson.get("tp_dollar") : null,
                                        orderJson.get("sl_percentage") != null && ((Double) orderJson.get("sl_percentage")) != 0
                                                ? (Double) orderJson.get("sl_percentage") : null,
                                        orderJson.get("tp_percentage") != null && ((Double) orderJson.get("tp_percentage")) != 0
                                                ? (Double) orderJson.get("tp_percentage") : null,
                                        minTick);
                                tp = tpSl[0];
                                sl = tpSl[1];
                            }
                            order.auxPrice(sl);
                        } else if ("TRAIL".equals(String.valueOf(order.orderType()))) {
                            order.trailStopPrice(orderJson.get("trailing_amount") != null &&
                                    ((Double) orderJson.get("trailing_amount")) != 0
                                    ? (Double) orderJson.get("trailing_amount") : null);
                        }
                        order.ocaGroup(ocaGroupId);
                        order.ocaType(1);
                        order.orderRef("SL");
                        TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, order);
                        Order executedOrder = result.getOrder();
                        orderToContractMap.put(executedOrder.orderId(), ibContract);
                        if (executedOrder != null) {
                            try {
//                                Thread.sleep(1000);
                                OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                                if (clients != null) {
                                    Map<String, Object> updateFields = new HashMap<>();
                                    updateFields.put("sl_temp_id", String.valueOf(executedOrder.orderId()));
                                    updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                    DatabaseConfig.updateOrderClient(clients, updateFields);
                                }
                            } catch (SQLException e) {
                                log.error("Error updating SL order: {}", e.getMessage());
                            }
                        }
                    }
                }
            }
        }
    }

    // Remaining methods (placeRemainingTpSlOrder, saveOrderToDatabase, getTradeData, orderToDict, generateRandomKey, getBestStrikeForOption, getTpSlPrice, closeLoop, monitorAndUpdateStopLoss) remain unchanged
    // For brevity, they are not repeated here, but they are assumed to be included as in the original code

    public void placeRemainingTpSlOrder(long time_var) {
        log.info("Starting placeRemainingTpSlOrder");
        List<OrderClient> allOrders;
        try {
            allOrders = new ArrayList<>(DatabaseConfig.getAllOrderClients(time_var));
        } catch (SQLException e) {
            log.error("Error retrieving all orders: {}", e.getMessage());
            return;
        }
        for (OrderClient order : allOrders) {
            

            try {
                String account = order.getAccountId();
                Map<String, Object> contractJson = gson.fromJson(order.getContractJson(), new TypeToken<Map<String, Object>>(){}.getType());
                Map<String, Object> orderJson = gson.fromJson(order.getOrderJson(), new TypeToken<Map<String, Object>>(){}.getType());
                String orderRandomId = order.getOrdersRandomId();
                handleOrderRecovery(orderRandomId, account, contractJson, orderJson, 1);
            } catch (Exception e) {
                log.error("Error placing TP/SL order for {}: {}", order.getContractJson(), e.getMessage());
            }
        }
        log.info("placeRemainingTpSlOrder completed");
    }

    private void saveOrderToDatabase(Map<String, Object> orderJson, Map<String, Object> contracts, String orderRandomId,
                                     Double entryOrderPrice, String orderId, String entryOrderId, String entryOrderStatus,
                                     Map<String, Object> contractJson, String errorMessage, Integer remaining) {
        log.debug("Saving order to database for orderRandomId , orderId: {}, {}", orderRandomId, orderId);
        OrderClient oc = new OrderClient();
        oc.setContractJson(gson.toJson(contractJson));
        oc.setOrderJson(gson.toJson(orderJson));
        oc.setAccountId((String) contracts.get("account"));
        oc.setQuantity(((Number) orderJson.get("quantity")).intValue());
        oc.setOrdersRandomId(orderRandomId);
        oc.setEntryPrice(entryOrderPrice != null ? entryOrderPrice.floatValue() : null);
        oc.setParentId(orderId);
        oc.setEntryId(entryOrderId);
        oc.setEntryStatus(entryOrderStatus);
        oc.setActive(true);
        Instant nowUtc = Instant.now();
        LocalDateTime utcDateTime = LocalDateTime.ofInstant(nowUtc, ZoneOffset.UTC);
        oc.setCreatedAt(utcDateTime);
        oc.setSymbol((String) contractJson.get("symbol"));
        oc.setExchange((String) contractJson.get("exchange"));
        oc.setCurrency((String) contractJson.get("currency"));
        oc.setMaturityDate((String) contractJson.get("maturityDate"));
        oc.setTradingClass((String) contractJson.get("trading_class"));
        oc.setAction((String) orderJson.get("action"));
        oc.setOrderType((String) orderJson.get("trade_type"));
        oc.setPrice(entryOrderPrice != null ? String.valueOf(entryOrderPrice) : null);
        oc.setTpPrice(orderJson.get("tp_price") != null && ((Double) orderJson.get("tp_price")) != 0
                ? ((Double) orderJson.get("tp_price")).floatValue() : null);
        oc.setCallPut((String) contractJson.get("right"));
        oc.setStrike(contractJson.get("strike") != null ? String.valueOf(contractJson.get("strike")) : "");
        oc.setSlPrice(orderJson.get("sl_price") != null && ((Double) orderJson.get("sl_price")) != 0
                ? ((Double) orderJson.get("sl_price")).floatValue() : null);
        oc.setErrorMessage(errorMessage);
        oc.setRemaining(remaining != null ? remaining.floatValue() : 0.0f);
        try {
            DatabaseConfig.saveOrderClient(oc);
            log.debug("Order saved to database: {}", orderRandomId);
        } catch (SQLException e) {
            log.error("Error saving order to database: {}", e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> getTradeData() {
        log.info("Fetching trade data not sent to server");
        List<OrderClient> allOrderData;
        try {
            allOrderData = new ArrayList<>(DatabaseConfig.getOrderClientsNotSentToServer());
        } catch (SQLException e) {
            log.error("Error retrieving trade data: {}", e.getMessage());
            return new ArrayList<>();
        }
        return allOrderData.stream().map(this::orderToDict).collect(Collectors.toList());
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

    private String generateRandomKey(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(random.nextInt(characters.length())));
        }
        return sb.toString();
    }


    private double getBestStrikeForOption(Map<String, Object> contractDetails, double strikeStart, double strikeEnd,
                                          double strikeInterval, double premiumStart, double premiumEnd) {
        Map<Double, double[]> optionData = new HashMap<>();
        for (double m = strikeStart * 10; m <= strikeEnd * 10; m += strikeInterval * 10) {
            double i = m / 10;
            Map<String, Object> contracss = new HashMap<>(contractDetails);
            contracss.put("strike", i);
            Contract ibContract = twsEngine.createContract(
                    (String) contracss.get("inst_type"),
                    (String) contracss.get("symbol"),
                    (String) contracss.get("exchange"),
                    (String) contracss.get("currency"),
                    (double) contracss.get("strike"),
                    (String) contracss.get("right"),
                    (String) contracss.get("symbol"),
                    (String) contracss.get("maturityDate"),
                    (String) contracss.get("trading_class"));
            Map<String, Double> data = twsEngine.getOptionDetails(ibContract).join();
            if (data != null && !Double.isNaN(data.get("bid"))) {
                optionData.put(i, new double[]{data.get("bid"), data.get("volume")});
            }
        }

        Map<Double, double[]> filteredStrikes = optionData.entrySet().stream()
                .filter(e -> !Double.isNaN(e.getValue()[0]) && !Double.isNaN(e.getValue()[1]))
                .filter(e -> e.getValue()[0] >= premiumStart && e.getValue()[0] <= premiumEnd)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (filteredStrikes.isEmpty()) return strikeEnd;

        return filteredStrikes.entrySet().stream()
                .max(Comparator.comparingDouble(e -> e.getValue()[1]))
                .map(Map.Entry::getKey)
                .orElse(strikeEnd);
    }

    private double[] getTpSlPrice(String instType, double entryPrice, String entryOrder, Double takeProfitPrice,
                                  Double stopLossPrice, Double slDollar, Double tpDollar, Double slPercentage,
                                  Double tpPercentage, double minTick) {
        double tp = 0;
        double sl = 0;

        if (tpDollar != null && tpDollar != 0 && tpPercentage == null) {
            tp = "BUY".equals(entryOrder) ? entryPrice + tpDollar : entryPrice - tpDollar;
        } else if (tpDollar == null && tpPercentage != null && tpPercentage != 0) {
            tp = "BUY".equals(entryOrder) ? entryPrice + (entryPrice * tpPercentage / 100) : entryPrice - (entryPrice * tpPercentage / 100);
        } else if (takeProfitPrice != null && takeProfitPrice != 0) {
            tp = takeProfitPrice;
        }

        if (slDollar != null && slDollar != 0 && slPercentage == null) {
            sl = "BUY".equals(entryOrder) ? entryPrice - slDollar : entryPrice + slDollar;
        } else if (slDollar == null && slPercentage != null && slPercentage != 0) {
            sl = "BUY".equals(entryOrder) ? entryPrice - (entryPrice * slPercentage / 100) : entryPrice + (entryPrice * slPercentage / 100);
        } else if (stopLossPrice != null && stopLossPrice != 0) {
            sl = stopLossPrice;
        }

        tp = Math.round(Math.round(tp / minTick) * minTick * 10000000000.0) / 10000000000.0;
        sl = Math.round(Math.round(sl / minTick) * minTick * 10000000000.0) / 10000000000.0;
        return new double[]{tp, sl};
    }



    private void monitorAndUpdateStopLoss(double entryOrderPrice, Order stopOrder, Contract ibContract,
                                          Map<String, Object> orderJson, double breakEven, String orderId,
                                          boolean newBreakEvenOrder, Map<String, Object> contracts) {
        executor.submit(() -> {
            while (true) {
                try {


                    double[] candle = twsEngine.reqLastCandle(ibContract, "1 D", "1 min").join();
                    double high = candle[0];
                    double low = candle[1];
                    boolean breakoutCondition;
                    double currentReference;
                    if ("BUY".equalsIgnoreCase((String) orderJson.get("action"))) {
                        breakoutCondition = high >= entryOrderPrice + breakEven;
                        currentReference = high;
                    } else {
                        breakoutCondition = low <= entryOrderPrice - breakEven;
                        currentReference = low;
                    }

                    log.info("Current reference price from last candle: {}", currentReference);

                    if (breakoutCondition && stopOrder != null) {
                        log.info("Breakout condition met. Updating stop-loss to break-even.");
                        stopOrder.auxPrice(entryOrderPrice);
                        stopOrder.transmit(true);
                        TwsEngine.OrderExecutionResult result = twsEngine.executeOrder(ibContract, stopOrder);
                        Order executedOrder = result.getOrder();
                        orderToContractMap.put(executedOrder.orderId(), ibContract);
                        if (executedOrder != null) {
//                            Thread.sleep(1000);
                            log.info("Stop-loss order successfully updated.");
                            OrderClient clients = DatabaseConfig.getOrderClientByParentId(orderId);
                            if (clients != null) {
                                Map<String, Object> updateFields = new HashMap<>();
                                updateFields.put("sl_temp_id", String.valueOf(executedOrder.orderId()));
                                updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Initialized.toString());
                                DatabaseConfig.updateOrderClient(clients, updateFields);
                            }
                        }
                    }

                    if (breakoutCondition && newBreakEvenOrder) {
                        while (true) {
                            
                            OrderClient stopOrderData = DatabaseConfig.getOrderClientByParentId(orderId);
                            String stopOrderStatus = stopOrderData != null ? stopOrderData.getSlStatus() : "Unknown";
                            if ("Filled".equals(stopOrderStatus)) break;
                            if (Arrays.asList("Cancelled", "Rejected", "Inactive").contains(stopOrderStatus)) {
                                log.error("Stop-loss order {}, cannot proceed.", stopOrderStatus.toLowerCase());
                                throw new IllegalStateException("Break-even order " + stopOrderStatus.toLowerCase() + ", cannot proceed.");
                            }
                            Thread.sleep(100);
                        }

                        String newAction = "BUY".equalsIgnoreCase((String) orderJson.get("action")) ? "SELL" : "BUY";
                        Map<String, Object> newContracts = new HashMap<>(contracts);
                        newContracts.put("order_details", new HashMap<>(orderJson));
                        ((Map<String, Object>) newContracts.get("order_details")).put("action", newAction);
                        ((Map<String, Object>) newContracts.get("order_details")).put("reverse_order_close", false);
                        newContracts.put("break_even", 0);
                        newContracts.put("new_break_even_order", false);

//                        Thread.sleep(5000);
                        placeTrade(newContracts).join();
                        return;
                    }

                    Thread.sleep(500);
                } catch (Exception e) {
                    log.error("Error in monitor_and_update_stop_loss: {}", e.getMessage());
                }
            }
        });
    }
}