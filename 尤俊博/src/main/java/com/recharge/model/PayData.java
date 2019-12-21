package com.recharge.model;

public class PayData {
    private String bussinessRst;//充值结果
    private Double chargefee;//充值金额
    private String receiveNotifyTime;//结束充值时间
    private String requestId;//开始充值时间
    private Integer provinceCode;//省份编号
    private String serviceName; //服务类型
    private Integer channelCode;
    private String clientIp;
    private String gateway_id;
    private String interFacRst;
    private String logOutTime;
    private String orderId;
    private String payPhoneNo;
    private String phoneno;
    private String rateoperateid;
    private String retMsg;
    private String serverIp;
    private Integer serverPort;
    private String shouldfee;
    private Integer srcChannel;
    private Integer sysId;

    public PayData() {
    }

    public PayData(String bussinessRst, Double chargefee, String receiveNotifyTime, String requestId, Integer provinceCode, String serviceName, Integer channelCode, String clientIp, String gateway_id, String interFacRst, String logOutTime, String orderId, String payPhoneNo, String phoneno, String rateoperateid, String retMsg, String serverIp, Integer serverPort, String shouldfee, Integer srcChannel, Integer sysId) {
        this.bussinessRst = bussinessRst;
        this.chargefee = chargefee;
        this.receiveNotifyTime = receiveNotifyTime;
        this.requestId = requestId;
        this.provinceCode = provinceCode;
        this.serviceName = serviceName;
        this.channelCode = channelCode;
        this.clientIp = clientIp;
        this.gateway_id = gateway_id;
        this.interFacRst = interFacRst;
        this.logOutTime = logOutTime;
        this.orderId = orderId;
        this.payPhoneNo = payPhoneNo;
        this.phoneno = phoneno;
        this.rateoperateid = rateoperateid;
        this.retMsg = retMsg;
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.shouldfee = shouldfee;
        this.srcChannel = srcChannel;
        this.sysId = sysId;
    }

    @Override
    public String toString() {
        return "PayData{" +
                "bussinessRst='" + bussinessRst + '\'' +
                ", chargefee=" + chargefee +
                ", receiveNotifyTime='" + receiveNotifyTime + '\'' +
                ", requestId='" + requestId + '\'' +
                ", provinceCode=" + provinceCode +
                ", serviceName='" + serviceName + '\'' +
                ", channelCode=" + channelCode +
                ", clientIp='" + clientIp + '\'' +
                ", gateway_id='" + gateway_id + '\'' +
                ", interFacRst='" + interFacRst + '\'' +
                ", logOutTime='" + logOutTime + '\'' +
                ", orderId='" + orderId + '\'' +
                ", payPhoneNo='" + payPhoneNo + '\'' +
                ", phoneno='" + phoneno + '\'' +
                ", rateoperateid='" + rateoperateid + '\'' +
                ", retMsg='" + retMsg + '\'' +
                ", serverIp='" + serverIp + '\'' +
                ", serverPort=" + serverPort +
                ", shouldfee='" + shouldfee + '\'' +
                ", srcChannel=" + srcChannel +
                ", sysId=" + sysId +
                '}';
    }

    public String getBussinessRst() {
        return bussinessRst;
    }

    public void setBussinessRst(String bussinessRst) {
        this.bussinessRst = bussinessRst;
    }

    public Double getChargefee() {
        return chargefee;
    }

    public void setChargefee(Double chargefee) {
        this.chargefee = chargefee;
    }

    public String getReceiveNotifyTime() {
        return receiveNotifyTime;
    }

    public void setReceiveNotifyTime(String receiveNotifyTime) {
        this.receiveNotifyTime = receiveNotifyTime;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Integer getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(Integer provinceCode) {
        this.provinceCode = provinceCode;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Integer getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(Integer channelCode) {
        this.channelCode = channelCode;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getGateway_id() {
        return gateway_id;
    }

    public void setGateway_id(String gateway_id) {
        this.gateway_id = gateway_id;
    }

    public String getInterFacRst() {
        return interFacRst;
    }

    public void setInterFacRst(String interFacRst) {
        this.interFacRst = interFacRst;
    }

    public String getLogOutTime() {
        return logOutTime;
    }

    public void setLogOutTime(String logOutTime) {
        this.logOutTime = logOutTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPayPhoneNo() {
        return payPhoneNo;
    }

    public void setPayPhoneNo(String payPhoneNo) {
        this.payPhoneNo = payPhoneNo;
    }

    public String getPhoneno() {
        return phoneno;
    }

    public void setPhoneno(String phoneno) {
        this.phoneno = phoneno;
    }

    public String getRateoperateid() {
        return rateoperateid;
    }

    public void setRateoperateid(String rateoperateid) {
        this.rateoperateid = rateoperateid;
    }

    public String getRetMsg() {
        return retMsg;
    }

    public void setRetMsg(String retMsg) {
        this.retMsg = retMsg;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public Integer getServerPort() {
        return serverPort;
    }

    public void setServerPort(Integer serverPort) {
        this.serverPort = serverPort;
    }

    public String getShouldfee() {
        return shouldfee;
    }

    public void setShouldfee(String shouldfee) {
        this.shouldfee = shouldfee;
    }

    public Integer getSrcChannel() {
        return srcChannel;
    }

    public void setSrcChannel(Integer srcChannel) {
        this.srcChannel = srcChannel;
    }

    public Integer getSysId() {
        return sysId;
    }

    public void setSysId(Integer sysId) {
        this.sysId = sysId;
    }
}
