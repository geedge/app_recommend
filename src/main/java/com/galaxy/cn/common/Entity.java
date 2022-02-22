package com.galaxy.cn.common;

import java.io.Serializable;

public class Entity implements Serializable {

    public int ifError;
    public long common_processing_time ;
    public String common_client_ip ;
    public String common_app_label ;
    public long common_sessions;

    public Entity() {
    }

    public int getIfError() {
        return ifError;
    }

    public void setIfError(int ifError) {
        this.ifError = ifError;
    }

    public long getCommon_recv_time() {
        return common_processing_time;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_processing_time = common_recv_time;
    }

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }
}
