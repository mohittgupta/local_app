package com.pickmytrade.ibapp.db.repository;

import com.pickmytrade.ibapp.db.DatabaseConfig;
import com.pickmytrade.ibapp.db.entities.ErrorLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class ErrorLogRepository {
    private static final Logger log = LoggerFactory.getLogger(ErrorLogRepository.class);

    public void saveErrorData(ErrorLog error) {
        try {
            DatabaseConfig.saveErrorData(error);
        } catch (SQLException e) {
            log.error("Error saving error data: {}", e.getMessage());
        }
    }

    public ErrorLog getErrorData(String contract) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM error_log_data WHERE contract = ? ORDER BY logged DESC LIMIT 1")) {
            pstmt.setString(1, contract);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ErrorLog error = new ErrorLog();
                    error.setId(rs.getInt("id"));
                    error.setReqId(rs.getString("req_id"));
                    error.setErrorCode(rs.getString("error_code"));
                    error.setErrorString(rs.getString("error_string"));
                    error.setContract(rs.getString("contract"));
                    error.setLogged(rs.getObject("logged", LocalDateTime.class));

                    LocalDateTime now = LocalDateTime.now();
                    if (ChronoUnit.SECONDS.between(error.getLogged(), now) <= 2) {
                        return error;
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving error data for contract {}: {}", contract, e.getMessage());
        }
        return null;
    }

    public ErrorLog getErrorDataById(String id) {
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT * FROM error_log_data WHERE req_id = ? ORDER BY logged DESC LIMIT 1")) {
            pstmt.setString(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ErrorLog error = new ErrorLog();
                    error.setId(rs.getInt("id"));
                    error.setReqId(rs.getString("req_id"));
                    error.setErrorCode(rs.getString("error_code"));
                    error.setErrorString(rs.getString("error_string"));
                    error.setContract(rs.getString("contract"));
                    error.setLogged(rs.getObject("logged", LocalDateTime.class));

                    LocalDateTime now = LocalDateTime.now();
                    if (ChronoUnit.SECONDS.between(error.getLogged(), now) <= 2) {
                        return error;
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Error retrieving error data by ID {}: {}", id, e.getMessage());
        }
        return null;
    }
}