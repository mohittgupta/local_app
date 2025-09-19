package com.pickmytrade.ibapp.db.entities;

import java.time.LocalDateTime;

public class ConnectionEntity {
    private Integer id;
    private String connectionName;
    private LocalDateTime createdAt = LocalDateTime.now();

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }
    public String getConnectionName() { return connectionName; }
    public void setConnectionName(String connectionName) { this.connectionName = connectionName; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}