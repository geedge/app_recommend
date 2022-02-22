package com.galaxy.cn.common;

public class CipEntity  {

    public long common_recv_time ;
    public String common_client_list ;
    public String common_app_label ;


    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getCommon_client_list() {
        return common_client_list;
    }

    public void setCommon_client_list(String common_client_list) {
        this.common_client_list = common_client_list;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public CipEntity() {
    }

}
