package com.pickmytrade.ibapp.db;

import com.pickmytrade.ibapp.db.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatabaseConfig {
    private static final Logger log = LoggerFactory.getLogger(DatabaseConfig.class);
    private static final String DB_URL;

    static {
        String platform = System.getProperty("os.name").toLowerCase();
        String appDataPath;

        if (platform.contains("mac")) {
            appDataPath = System.getProperty("user.home") + "/Library/Application Support";
        } else if (platform.contains("linux")) {
            appDataPath = System.getProperty("user.home") + "/.local/share";
        } else {
            appDataPath = System.getenv("APPDATA");
            if (appDataPath == null) {
                throw new RuntimeException("APPDATA environment variable not found");
            }
        }

        File pickMyTradeDir = new File(appDataPath, "PickMYTrade");

        // Create PickMYTrade directory if it doesn't exist
        if (!pickMyTradeDir.exists()) {
            boolean created = pickMyTradeDir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create directory: " + pickMyTradeDir.getAbsolutePath());
            }
        }

        File dbFile = new File(pickMyTradeDir, "IB.db");
        DB_URL = "jdbc:sqlite:" + dbFile.getAbsolutePath().replace("\\", "/");

        // Initialize database tables
        try (java.sql.Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            // Create Token table
            stmt.execute("CREATE TABLE IF NOT EXISTS tokens (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "token TEXT NOT NULL, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            // Create Connection table
            stmt.execute("CREATE TABLE IF NOT EXISTS connections (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "connection_name TEXT NOT NULL, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            // Create AccountData table
            stmt.execute("CREATE TABLE IF NOT EXISTS account_data (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "account_id TEXT NOT NULL, " +
                    "data TEXT, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            // Create OrderClient table
            stmt.execute("CREATE TABLE IF NOT EXISTS order_clients (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "orders_random_id TEXT, " +
                    "client_db_id INTEGER, " +
                    "client_name TEXT, " +
                    "account_id TEXT, " +
                    "risk_multiplier TEXT, " +
                    "fund REAL, " +
                    "max_stock INTEGER, " +
                    "contract_json TEXT, " +
                    "order_json TEXT, " +
                    "remaining REAL DEFAULT 0, " +
                    "rm_option REAL, " +
                    "rm_stock REAL, " +
                    "quantity INTEGER, " +
                    "parent_id TEXT, " +
                    "entry_price REAL, " +
                    "entry_filled_price REAL, " +
                    "tp_filled_price REAL, " +
                    "tp_price REAL, " +
                    "sl_price REAL, " +
                    "strike TEXT, " +
                    "entry_id TEXT, " +
                    "tp_temp_id TEXT, " +
                    "sl_temp_id TEXT, " +
                    "tp_id TEXT, " +
                    "sl_id TEXT, " +
                    "entry_status TEXT, " +
                    "tp_status TEXT, " +
                    "sl_status TEXT, " +
                    "active BOOLEAN, " +
                    "created_at TIMESTAMP, " +
                    "symbol TEXT, " +
                    "exchange TEXT, " +
                    "currency TEXT, " +
                    "maturity_date TEXT, " +
                    "trading_class TEXT, " +
                    "call_put TEXT, " +
                    "action TEXT, " +
                    "security_type TEXT, " +
                    "order_type TEXT, " +
                    "price TEXT, " +
                    "error_message TEXT, " +
                    "sent_to_server TEXT DEFAULT 'Initialized')");

            // Create ErrorLog table
            stmt.execute("CREATE TABLE IF NOT EXISTS error_log_data (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "req_id TEXT, " +
                    "error_code TEXT, " +
                    "error_string TEXT, " +
                    "contract TEXT, " +
                    "logged TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");


        } catch (SQLException e) {
            log.error("Failed to initialize database: {}", e.getMessage());
            throw new RuntimeException("Database initialization failed", e);
        }
    }

    // Get a connection to the SQLite database
    public static java.sql.Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL);
    }


    // Token methods
    public static Token getToken() throws SQLException {
        try (java.sql.Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, token, created_at FROM tokens LIMIT 1")) {
            if (rs.next()) {
                Token token = new Token();
                token.setId(rs.getInt("id"));
                token.setToken(rs.getString("token"));
                token.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                return token;
            }
            return null;
        }
    }

    public static void saveOrUpdateToken(String tokenValue) throws SQLException {
        try (java.sql.Connection conn = getConnection()) {
            String sql = "SELECT id FROM tokens LIMIT 1";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    // Update existing token
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "UPDATE tokens SET token = ?, created_at = ? WHERE id = ?")) {
                        pstmt.setString(1, tokenValue);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setInt(3, rs.getInt("id"));
                        pstmt.executeUpdate();
                    }
                } else {
                    // Insert new token
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "INSERT INTO tokens (token, created_at) VALUES (?, ?)")) {
                        pstmt.setString(1, tokenValue);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }

    // ConnectionEntity methods
    public static ConnectionEntity getConnectionEntity() throws SQLException {
        try (java.sql.Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, connection_name, created_at FROM connections LIMIT 1")) {
            if (rs.next()) {
                ConnectionEntity connection = new ConnectionEntity();
                connection.setId(rs.getInt("id"));
                connection.setConnectionName(rs.getString("connection_name"));
                connection.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                return connection;
            }
            return null;
        }
    }

    public static void emptyconnectionsTable() {
        String sql = "DELETE FROM connections";
        try (Connection conn = getConnection(); // Assume getConnection() is a method that returns a database connection
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int rowsAffected = pstmt.executeUpdate();
            log.info("Successfully reset connections table. Rows deleted: {}", rowsAffected);
        } catch (SQLException e) {
            log.error("Error reset connections table: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to reset connections table", e);
        }
    }

    public static void saveConnection(String connectionName) throws SQLException {
        try (java.sql.Connection conn = getConnection()) {
            String sql = "SELECT id FROM connections LIMIT 1";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "INSERT INTO connections (connection_name, created_at) VALUES (?, ?)")) {
                        pstmt.setString(1, connectionName);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.executeUpdate();
                    }
                } else {
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "UPDATE connections SET connection_name = ?, created_at = ? WHERE id = ?")) {
                        pstmt.setString(1, connectionName);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setInt(3, rs.getInt("id"));
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }


    // AccountData methods
    public static List<AccountData> getAccountData() throws SQLException {
        List<AccountData> accountDataList = new ArrayList<>();
        try (java.sql.Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, account_id, data, created_at FROM account_data")) {
            while (rs.next()) {
                AccountData accountData = new AccountData();
                accountData.setId(rs.getInt("id"));
                accountData.setAccountId(rs.getString("account_id"));
                accountData.setData(rs.getString("data"));
                accountData.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                accountDataList.add(accountData);
            }
        }
        return accountDataList;
    }

    public static void saveAccountData(AccountData accountData) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "INSERT INTO account_data (account_id, data, created_at) VALUES (?, ?, ?)")) {
            pstmt.setString(1, accountData.getAccountId());
            pstmt.setString(2, accountData.getData());
            pstmt.setObject(3, LocalDateTime.now());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to save account data: {}", e.getMessage());
            throw e;
        }
    }

    public static void emptyAccountDataTable() throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "DELETE FROM account_data")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to empty account data table: {}", e.getMessage());
            throw e;
        }
    }

    // OrderClient methods (replacing OrderClientRepository)
    public static void saveOrderClient(OrderClient orderClient) throws SQLException {
        try (java.sql.Connection conn = getConnection()) {
            String sql = "INSERT INTO order_clients (orders_random_id, client_db_id, client_name, account_id, " +
                    "risk_multiplier, fund, max_stock, contract_json, order_json, remaining, rm_option, rm_stock, " +
                    "quantity, parent_id, entry_price, entry_filled_price, tp_filled_price, tp_price, sl_price, " +
                    "strike, entry_id, tp_temp_id, sl_temp_id, tp_id, sl_id, entry_status, tp_status, sl_status, " +
                    "active, created_at, symbol, exchange, currency, maturity_date, trading_class, call_put, action, " +
                    "security_type, order_type, price, error_message, sent_to_server) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, orderClient.getOrdersRandomId());
                pstmt.setObject(2, orderClient.getClientDbId());
                pstmt.setString(3, orderClient.getClientName());
                pstmt.setString(4, orderClient.getAccountId());
                pstmt.setString(5, orderClient.getRiskMultiplier());
                pstmt.setObject(6, orderClient.getFund());
                pstmt.setObject(7, orderClient.getMaxStock());
                pstmt.setString(8, orderClient.getContractJson());
                pstmt.setString(9, orderClient.getOrderJson());
                pstmt.setFloat(10, orderClient.getRemaining());
                pstmt.setObject(11, orderClient.getRmOption());
                pstmt.setObject(12, orderClient.getRmStock());
                pstmt.setObject(13, orderClient.getQuantity());
                pstmt.setString(14, orderClient.getParentId());
                pstmt.setObject(15, orderClient.getEntryPrice());
                pstmt.setObject(16, orderClient.getEntryFilledPrice());
                pstmt.setObject(17, orderClient.getTpFilledPrice());
                pstmt.setObject(18, orderClient.getTpPrice());
                pstmt.setObject(19, orderClient.getSlPrice());
                pstmt.setString(20, orderClient.getStrike());
                pstmt.setString(21, orderClient.getEntryId());
                pstmt.setString(22, orderClient.getTpTempId());
                pstmt.setString(23, orderClient.getSlTempId());
                pstmt.setString(24, orderClient.getTpId());
                pstmt.setString(25, orderClient.getSlId());
                pstmt.setString(26, orderClient.getEntryStatus());
                pstmt.setString(27, orderClient.getTpStatus());
                pstmt.setString(28, orderClient.getSlStatus());
                pstmt.setObject(29, orderClient.getActive());
                pstmt.setObject(30, orderClient.getCreatedAt());
                pstmt.setString(31, orderClient.getSymbol());
                pstmt.setString(32, orderClient.getExchange());
                pstmt.setString(33, orderClient.getCurrency());
                pstmt.setString(34, orderClient.getMaturityDate());
                pstmt.setString(35, orderClient.getTradingClass());
                pstmt.setString(36, orderClient.getCallPut());
                pstmt.setString(37, orderClient.getAction());
                pstmt.setString(38, orderClient.getSecurityType());
                pstmt.setString(39, orderClient.getOrderType());
                pstmt.setString(40, orderClient.getPrice());
                pstmt.setString(41, orderClient.getErrorMessage());
                pstmt.setString(42, orderClient.getSentToServer() != null ? orderClient.getSentToServer().name() : "Initialized");
                pstmt.executeUpdate();
            }
        }
    }

    public static OrderClient getOrderClientByParentId(String parentId) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM order_clients WHERE parent_id = ? LIMIT 1")) {
            pstmt.setString(1, parentId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        }
        return null;
    }

    public static OrderClient getOrderClientByOrderRandomId(String orderRandomId) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM order_clients WHERE orders_random_id = ? LIMIT 1")) {
            pstmt.setString(1, orderRandomId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        }
        return null;
    }

    public static OrderClient getOrderClientByTpTempId(String tpTempId) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM order_clients WHERE tp_temp_id = ? LIMIT 1")) {
            pstmt.setString(1, tpTempId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        }
        return null;
    }

    public static OrderClient getOrderClientBySlTempId(String slTempId) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM order_clients WHERE sl_temp_id = ? LIMIT 1")) {
            pstmt.setString(1, slTempId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToOrderClient(rs);
                }
            }
        }
        return null;
    }

    public static List<OrderClient> getAllOrderClients(long time_var) throws SQLException {
        List<OrderClient> orders = new ArrayList<>();

        // Convert milliseconds to LocalDateTime
        LocalDateTime dateTime = Instant.ofEpochMilli(time_var)
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();

        String sql = "SELECT * FROM order_clients WHERE active = 1 AND datetime(created_at) >= datetime(?)";

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, dateTime.toString().replace('T', ' ')); // format: "YYYY-MM-DD HH:MM:SS"

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapResultSetToOrderClient(rs));
                }
            }
        }

        return orders;
    }

    public static List<OrderClient> getOrderClientsNotSentToServer() throws SQLException {
        List<OrderClient> orders = new ArrayList<>();

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT *, " +
                             "strftime('%s', REPLACE(substr(created_at, 1, 19), 'T', ' ')) AS created_ts, " +
                             "strftime('%s', 'now', '-20 seconds') AS cutoff_ts " +
                             "FROM order_clients " +
                             "WHERE LOWER(TRIM(sent_to_server)) != 'pushed' " +
                             "   OR strftime('%s', REPLACE(substr(created_at, 1, 19), 'T', ' ')) >= strftime('%s', 'now', '-20 seconds')"
             )) {

            while (rs.next()) {
                // Debug log
                String createdAt = rs.getString("created_at");
                String createdTs = rs.getString("created_ts");
                String cutoffTs = rs.getString("cutoff_ts");
                String sentToServer = rs.getString("sent_to_server");

                System.out.println("created_at: " + createdAt +
                        " | created_ts: " + createdTs +
                        " | cutoff_ts: " + cutoffTs +
                        " | sent_to_server: " + sentToServer);

                orders.add(mapResultSetToOrderClient(rs));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return orders;
    }



    public static void emptyOrderClientTable() {
        String sql = "DELETE FROM order_clients";
        try (Connection conn = getConnection(); // Assume getConnection() is a method that returns a database connection
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int rowsAffected = pstmt.executeUpdate();
            log.info("Successfully emptied OrderClient table. Rows deleted: {}", rowsAffected);
        } catch (SQLException e) {
            log.error("Error emptying OrderClient table: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to empty OrderClient table", e);
        }
    }


    public static void updateOrderClient(OrderClient orderClient, Map<String, Object> updateFields) throws SQLException {
        if (orderClient == null) {
            log.error("OrderClient is null, cannot update");
            throw new IllegalArgumentException("OrderClient cannot be null");
        }

        if (orderClient.getId() <= 0) {
            log.warn("Invalid OrderClient ID: {}, skipping update", orderClient.getId());
            return;
        }

        if (updateFields == null || updateFields.isEmpty()) {
            log.warn("Update fields dictionary is null or empty, skipping update");
            return;
        }

        log.info("Attempting to update OrderClient with id={}", orderClient.getId());
        log.debug("Update fields provided: {}", updateFields);

        StringBuilder sql = new StringBuilder("UPDATE order_clients SET ");
        int index = 1;
        for (String key : updateFields.keySet()) {
            if (index > 1) {
                sql.append(", ");
            }
            sql.append(key).append(" = ?");
            index++;
        }
        sql.append(" WHERE id = ?");

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            index = 1;
            for (Map.Entry<String, Object> entry : updateFields.entrySet()) {
                pstmt.setObject(index, entry.getValue());
                index++;
            }
            pstmt.setInt(index, orderClient.getId());

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected == 0) {
                log.warn("No rows updated for OrderClient id={}", orderClient.getId());
            } else {
                log.info("Successfully updated {} row(s) for OrderClient id={}", rowsAffected, orderClient.getId());
            }
        } catch (SQLException e) {
            log.error("Failed to update OrderClient id={}: {}", orderClient.getId(), e.getMessage(), e);
            throw e;
        }
    }

    private static OrderClient mapResultSetToOrderClient(ResultSet rs) throws SQLException {
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

    // ErrorLog methods (replacing ErrorLogRepository)
    public static void saveErrorData(ErrorLog errorLog) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "INSERT INTO error_log_data (req_id, error_code, error_string, contract, logged) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setString(1, errorLog.getReqId());
            pstmt.setString(2, errorLog.getErrorCode());
            pstmt.setString(3, errorLog.getErrorString());
            pstmt.setString(4, errorLog.getContract());
            pstmt.setObject(5, errorLog.getLogged());
            pstmt.executeUpdate();
        }
    }

    public static ErrorLog getErrorData(String contract) throws SQLException {
        try (java.sql.Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM error_log_data WHERE contract = ? LIMIT 1")) {
            pstmt.setString(1, contract);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ErrorLog errorLog = new ErrorLog();
                    errorLog.setId(rs.getInt("id"));
                    errorLog.setReqId(rs.getString("req_id"));
                    errorLog.setErrorCode(rs.getString("error_code"));
                    errorLog.setErrorString(rs.getString("error_string"));
                    errorLog.setContract(rs.getString("contract"));
                    errorLog.setLogged(rs.getObject("logged", LocalDateTime.class));
                    return errorLog;
                }
            }
        }
        return null;
    }
}