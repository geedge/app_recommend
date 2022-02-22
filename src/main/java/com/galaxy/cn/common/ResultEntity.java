package com.galaxy.cn.common;

public class ResultEntity implements Comparable<ResultEntity> {

    public String common_client_ip ;
    public long sessions ;
/*
    public String common_app_label ;
    public long common_recv_time ;
*/

/*    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }*/

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public long getSessions() {
        return sessions;
    }

    public void setSessions(long sessions) {
        this.sessions = sessions;
    }

    @Override
    public int compareTo(ResultEntity per) {
        if(this.sessions<per.sessions){
            return -1 ;
        }else if(this.sessions>per.sessions){
            return 1 ;
        }else{
            return this.common_client_ip.compareTo(per.common_client_ip) ; // 调用String中的compareTo()方法
        }
    }



}
