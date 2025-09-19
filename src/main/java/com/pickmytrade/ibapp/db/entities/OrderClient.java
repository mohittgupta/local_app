package com.pickmytrade.ibapp.db.entities;

import java.time.LocalDateTime;

public class OrderClient {
    private Integer id;
    private String ordersRandomId;
    private Integer clientDbId;
    private String clientName;
    private String accountId;
    private String riskMultiplier;
    private Float fund;
    private Integer maxStock;
    private String contractJson;
    private String orderJson;
    private Float remaining = (float) 0;
    private Float rmOption;
    private Float rmStock;
    private Integer quantity;
    private String parentId;
    private Float entryPrice;
    private Float entryFilledPrice;
    private Float tpFilledPrice;
    private Float tpPrice;
    private Float slPrice;
    private String strike;
    private String entryId;
    private String tpTempId;
    private String slTempId;
    private String tpId;
    private String slId;
    private String entryStatus;
    private String tpStatus;
    private String slStatus;
    private Boolean active;
    private LocalDateTime createdAt;
    private String symbol;
    private String exchange;
    private String currency;
    private String maturityDate;
    private String tradingClass;
    private String callPut;
    private String action;
    private String securityType;
    private String orderType;
    private String price;
    private String errorMessage;
    private SentToServerStatus sentToServer = SentToServerStatus.Initialized;

    public enum SentToServerStatus {
        Initialized, Pushed, Failed
    }

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }
    public String getOrdersRandomId() { return ordersRandomId; }
    public void setOrdersRandomId(String ordersRandomId) { this.ordersRandomId = ordersRandomId; }
    public Integer getClientDbId() { return clientDbId; }
    public void setClientDbId(Integer clientDbId) { this.clientDbId = clientDbId; }
    public String getClientName() { return clientName; }
    public void setClientName(String clientName) { this.clientName = clientName; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public String getRiskMultiplier() { return riskMultiplier; }
    public void setRiskMultiplier(String riskMultiplier) { this.riskMultiplier = riskMultiplier; }
    public Float getFund() { return fund; }
    public void setFund(Float fund) { this.fund = fund; }
    public Integer getMaxStock() { return maxStock; }
    public void setMaxStock(Integer maxStock) { this.maxStock = maxStock; }
    public String getContractJson() { return contractJson; }
    public void setContractJson(String contractJson) { this.contractJson = contractJson; }
    public String getOrderJson() { return orderJson; }
    public void setOrderJson(String orderJson) { this.orderJson = orderJson; }
    public Float getRemaining() { return remaining; }
    public void setRemaining(Float remaining) { this.remaining = remaining; }
    public Float getRmOption() { return rmOption; }
    public void setRmOption(Float rmOption) { this.rmOption = rmOption; }
    public Float getRmStock() { return rmStock; }
    public void setRmStock(Float rmStock) { this.rmStock = rmStock; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    public String getParentId() { return parentId; }
    public void setParentId(String parentId) { this.parentId = parentId; }
    public Float getEntryPrice() { return entryPrice; }
    public void setEntryPrice(Float entryPrice) { this.entryPrice = entryPrice; }
    public Float getEntryFilledPrice() { return entryFilledPrice; }
    public void setEntryFilledPrice(Float entryFilledPrice) { this.entryFilledPrice = entryFilledPrice; }
    public Float getTpFilledPrice() { return tpFilledPrice; }
    public void setTpFilledPrice(Float tpFilledPrice) { this.tpFilledPrice = tpFilledPrice; }
    public Float getTpPrice() { return tpPrice; }
    public void setTpPrice(Float tpPrice) { this.tpPrice = tpPrice; }
    public Float getSlPrice() { return slPrice; }
    public void setSlPrice(Float slPrice) { this.slPrice = slPrice; }
    public String getStrike() { return strike; }
    public void setStrike(String strike) { this.strike = strike; }
    public String getEntryId() { return entryId; }
    public void setEntryId(String entryId) { this.entryId = entryId; }
    public String getTpTempId() { return tpTempId; }
    public void setTpTempId(String tpTempId) { this.tpTempId = tpTempId; }
    public String getSlTempId() { return slTempId; }
    public void setSlTempId(String slTempId) { this.slTempId = slTempId; }
    public String getTpId() { return tpId; }
    public void setTpId(String tpId) { this.tpId = tpId; }
    public String getSlId() { return slId; }
    public void setSlId(String slId) { this.slId = slId; }
    public String getEntryStatus() { return entryStatus; }
    public void setEntryStatus(String entryStatus) { this.entryStatus = entryStatus; }
    public String getTpStatus() { return tpStatus; }
    public void setTpStatus(String tpStatus) { this.tpStatus = tpStatus; }
    public String getSlStatus() { return slStatus; }
    public void setSlStatus(String slStatus) { this.slStatus = slStatus; }
    public Boolean getActive() { return active; }
    public void setActive(Boolean active) { this.active = active; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getMaturityDate() { return maturityDate; }
    public void setMaturityDate(String maturityDate) { this.maturityDate = maturityDate; }
    public String getTradingClass() { return tradingClass; }
    public void setTradingClass(String tradingClass) { this.tradingClass = tradingClass; }
    public String getCallPut() { return callPut; }
    public void setCallPut(String callPut) { this.callPut = callPut; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public String getSecurityType() { return securityType; }
    public void setSecurityType(String securityType) { this.securityType = securityType; }
    public String getOrderType() { return orderType; }
    public void setOrderType(String orderType) { this.orderType = orderType; }
    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public SentToServerStatus getSentToServer() { return sentToServer; }
    public void setSentToServer(SentToServerStatus sentToServer) { this.sentToServer = sentToServer; }
}