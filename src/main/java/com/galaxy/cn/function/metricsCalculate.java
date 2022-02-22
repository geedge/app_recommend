package com.galaxy.cn.function;

import com.galaxy.cn.common.Entity;
import com.galaxy.cn.common.ObjectEntity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public  class metricsCalculate extends ProcessWindowFunction<
        Entity,                  // 输入类型
        ObjectEntity,  // 输出类型
        Tuple2<String,String>,                         // 键类型
        TimeWindow> {                   // 窗口类型
    @Override
    public void process(Tuple2<String,String> s,
                        Context context,
                        Iterable<Entity> elements, Collector<ObjectEntity> out) throws Exception {

        ObjectEntity objEntity= new ObjectEntity();
        objEntity.setCommon_recv_time(context.window().getEnd()/1000);
        objEntity.setCommon_app_label(s.f0);
        objEntity.setCommon_client_ip(s.f1);

        for (Entity event : elements) {
            objEntity.sessions+=event.common_sessions;
        }
        out.collect(objEntity);
    }
}