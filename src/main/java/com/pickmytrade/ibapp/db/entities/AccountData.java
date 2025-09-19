package com.pickmytrade.ibapp.db.entities;

import java.time.LocalDateTime;

public class AccountData {
    private Integer id;
    private String accountId;
    private String data;
    private LocalDateTime createdAt = LocalDateTime.now();

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public String getData() { return data; }
    public void setData(String data) { this.data = data; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}