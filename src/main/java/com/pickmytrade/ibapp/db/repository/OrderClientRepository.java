package com.pickmytrade.ibapp.db.repository;

import com.pickmytrade.ibapp.db.DatabaseConfig;
import com.pickmytrade.ibapp.db.entities.OrderClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class OrderClientRepository {
    private static final Logger log = LoggerFactory.getLogger(OrderClientRepository.class);

    public void saveOrderClient(OrderClient orderClient) {
        try {
            DatabaseConfig.saveOrderClient(orderClient);
        } catch (SQLException e) {
            log.error("Error saving order client: {}", e.getMessage());
        }
    }

    public void removeOrderClient(OrderClient orderClient) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("DELETE FROM order_clients WHERE id = ?")) {
            pstmt.setInt(1, orderClient.getId());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Error removing order client: {}", e.getMessage());
        }
    }

    public List<OrderClient> getOrderClientAlert() {
        List<OrderClient> orders = new ArrayList<>();
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE tp_temp_id IS NULL AND sl_temp_id IS NULL AND entry_status IN ('FILLED', 'SUBMITTED', 'PRESUBMITTED')")) {
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapResultSetToOrderClient(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client alerts: {}", e.getMessage());
        }
        return orders;
    }

    public List<OrderClient> getAllClientAlerts(String randomId) {
        List<OrderClient> orders = new ArrayList<>();
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE orders_random_id = ? AND entry_status IN ('FILLED', 'SUBMITTED', 'PRESUBMITTED')")) {
            pstmt.setString(1, randomId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapResultSetToOrderClient(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving all client alerts for randomId {}: {}", randomId, e.getMessage());
        }
        return orders;
    }

    public OrderClient getOrderClientAlertByOrderRandomId(String randomId) {
        try {
            return DatabaseConfig.getOrderClientByOrderRandomId(randomId);
        } catch (SQLException e) {
            log.error("Error retrieving order client by randomId {}: {}", randomId, e.getMessage());
            return null;
        }
    }

    public List<OrderClient> getOrderClientCompleteAlert(String randomId) {
        List<OrderClient> orders = new ArrayList<>();
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE orders_random_id = ? AND entry_status NOT IN ('SUBMITTED', 'FILLED')")) {
            pstmt.setString(1, randomId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapResultSetToOrderClient(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving completed order client alerts for randomId {}: {}", randomId, e.getMessage());
        }
        return orders;
    }

    public OrderClient getOrderClientAlertByEntry(String entryId) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE entry_id = ? ORDER BY id DESC LIMIT 1")) {
            pstmt.setString(1, entryId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client by entryId {}: {}", entryId, e.getMessage());
        }
        return null;
    }

    public OrderClient getOrderClientAlertByParentId(String parentId) {
        try {
            return DatabaseConfig.getOrderClientByParentId(parentId);
        } catch (SQLException e) {
            log.error("Error retrieving order client by parentId {}: {}", parentId, e.getMessage());
            return null;
        }
    }

    public List<OrderClient> getOrderClientAlertBySubmittedStatus() {
        List<OrderClient> orders = new ArrayList<>();
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE entry_status = 'SUBMITTED'")) {
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapResultSetToOrderClient(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order clients with submitted status: {}", e.getMessage());
        }
        return orders;
    }

    public OrderClient getOrderClientAlertById(Integer id) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE id = ? ORDER BY id DESC LIMIT 1")) {
            pstmt.setInt(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client by id {}: {}", id, e.getMessage());
        }
        return null;
    }

    public OrderClient getOrderClientAlertByAccountId(String accountId, String status) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE account_id = ? AND entry_status = ? ORDER BY id DESC LIMIT 1")) {
            pstmt.setString(1, accountId);
            pstmt.setString(2, status);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client by accountId {} and status {}: {}", accountId, status, e.getMessage());
        }
        return null;
    }

    public OrderClient getOrderClientAlertByTp(String tpId) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE tp_id = ? ORDER BY id DESC LIMIT 1")) {
            pstmt.setString(1, tpId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client by tpId {}: {}", tpId, e.getMessage());
        }
        return null;
    }

    public OrderClient getOrderClientAlertBySl(String slId) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM order_clients WHERE sl_id = ? ORDER BY id DESC LIMIT 1")) {
            pstmt.setString(1, slId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving order client by slId {}: {}", slId, e.getMessage());
        }
        return null;
    }

    public OrderClient getOrderClientAlertByTpTempId(String tpOrderId) {
        try {
            return DatabaseConfig.getOrderClientByTpTempId(tpOrderId);
        } catch (SQLException e) {
            log.error("Error retrieving order client by tpTempId {}: {}", tpOrderId, e.getMessage());
            return null;
        }
    }

    public OrderClient getOrderClientAlertBySlTempId(String slOrderId) {
        try {
            return DatabaseConfig.getOrderClientBySlTempId(slOrderId);
        } catch (SQLException e) {
            log.error("Error retrieving order client by slTempId {}: {}", slOrderId, e.getMessage());
            return null;
        }
    }

    public List<OrderClient> getOrderClientAlertNotSentToServer() {
        try {
            return DatabaseConfig.getOrderClientsNotSentToServer();
        } catch (SQLException e) {
            log.error("Error retrieving order clients not sent to server: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private OrderClient mapResultSetToOrderClient(ResultSet rs) throws SQLException {
        OrderClient oc = new OrderClient();
        oc.setId(rs.getInt("id"));
        oc.setOrdersRandomId(rs.getString("orders_random_id"));
        oc.setClientDbId(rs.getObject("client_db_id") != null ? rs.getInt("client_db_id") : null);
        oc.setClientName(rs.getString("client_name"));
        oc.setAccountId(rs.getString("account_id"));
        oc.setRiskMultiplier(rs.getString("risk_multiplier"));
        oc.setFund(rs.getObject("fund") != null ? rs.getFloat("fund") : null);
        oc.setMaxStock(rs.getObject("max_stock") != null ? rs.getInt("max_stock") : null);
        oc.setContractJson(rs.getString("contract_json"));
        oc.setOrderJson(rs.getString("order_json"));
        oc.setRemaining(rs.getFloat("remaining"));
        oc.setRmOption(rs.getObject("rm_option") != null ? rs.getFloat("rm_option") : null);
        oc.setRmStock(rs.getObject("rm_stock") != null ? rs.getFloat("rm_stock") : null);
        oc.setQuantity(rs.getObject("quantity") != null ? rs.getInt("quantity") : null);
        oc.setParentId(rs.getString("parent_id"));
        oc.setEntryPrice(rs.getObject("entry_price") != null ? rs.getFloat("entry_price") : null);
        oc.setEntryFilledPrice(rs.getObject("entry_filled_price") != null ? rs.getFloat("entry_filled_price") : null);
        oc.setTpFilledPrice(rs.getObject("tp_filled_price") != null ? rs.getFloat("tp_filled_price") : null);
        oc.setTpPrice(rs.getObject("tp_price") != null ? rs.getFloat("tp_price") : null);
        oc.setSlPrice(rs.getObject("sl_price") != null ? rs.getFloat("sl_price") : null);
        oc.setStrike(rs.getString("strike"));
        oc.setEntryId(rs.getString("entry_id"));
        oc.setTpTempId(rs.getString("tp_temp_id"));
        oc.setSlTempId(rs.getString("sl_temp_id"));
        oc.setTpId(rs.getString("tp_id"));
        oc.setSlId(rs.getString("sl_id"));
        oc.setEntryStatus(rs.getString("entry_status"));
        oc.setTpStatus(rs.getString("tp_status"));
        oc.setSlStatus(rs.getString("sl_status"));
        oc.setActive(rs.getObject("active") != null ? rs.getBoolean("active") : null);
        oc.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
        oc.setSymbol(rs.getString("symbol"));
        oc.setExchange(rs.getString("exchange"));
        oc.setCurrency(rs.getString("currency"));
        oc.setMaturityDate(rs.getString("maturity_date"));
        oc.setTradingClass(rs.getString("trading_class"));
        oc.setCallPut(rs.getString("call_put"));
        oc.setAction(rs.getString("action"));
        oc.setSecurityType(rs.getString("security_type"));
        oc.setOrderType(rs.getString("order_type"));
        oc.setPrice(rs.getString("price"));
        oc.setErrorMessage(rs.getString("error_message"));
        oc.setSentToServer(rs.getString("sent_to_server") != null ?
                OrderClient.SentToServerStatus.valueOf(rs.getString("sent_to_server")) : OrderClient.SentToServerStatus.Initialized);
        return oc;
    }
}