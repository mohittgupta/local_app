package com.pickmytrade.ibapp.db.entities;

import java.time.LocalDateTime;

public class ErrorLog {
    private Integer id;
    private String reqId;
    private String errorCode;
    private String errorString;
    private String contract;
    private LocalDateTime logged = LocalDateTime.now();

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }
    public String getReqId() { return reqId; }
    public void setReqId(String reqId) { this.reqId = reqId; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorString() { return errorString; }
    public void setErrorString(String errorString) { this.errorString = errorString; }
    public String getContract() { return contract; }
    public void setContract(String contract) { this.contract = contract; }
    public LocalDateTime getLogged() { return logged; }
    public void setLogged(LocalDateTime logged) { this.logged = logged; }
}