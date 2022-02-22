package com.galaxy.cn.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.cn.common.ObjectEntity;
import com.galaxy.cn.common.ResultEntity;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public  class TopNHotItems extends KeyedProcessFunction<Tuple1<String>, ObjectEntity, Tuple3<String,String,Long>> {
    private final int topSize;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private Map<String,PriorityQueue<ResultEntity>> resultMap ;
    private Map<String,Map<String,Long>> countMap ;

    public TopNHotItems(int i) {
        this.topSize = i;


    }
    @Override
    public void open(Configuration parameters) throws Exception {
        this.resultMap= new HashMap<>();
        this.countMap= new HashMap<>();

    }


    @Override
    public void processElement(ObjectEntity objectEntity, Context context, Collector<Tuple3<String,String,Long>> collector) throws Exception {
        //allSet.add(objectEntity);


        if(countMap.containsKey(objectEntity.getCommon_app_label())) {

            PriorityQueue<ResultEntity> queue =resultMap.get(objectEntity.getCommon_app_label());
            Map<String,Long> map = countMap.get(objectEntity.getCommon_app_label());
            if (map.size() < topSize) {
                ResultEntity resultEntity =new ResultEntity();
                resultEntity.setSessions(objectEntity.getSessions());
                resultEntity.setCommon_client_ip(objectEntity.getCommon_client_ip());
               // resultEntity.setCommon_app_label(objectEntity.getCommon_app_label());
               // resultEntity.setCommon_recv_time(objectEntity.getCommon_recv_time());
                queue.add(resultEntity);
                map.put(objectEntity.getCommon_client_ip(),objectEntity.getSessions());
            } else {
                if (queue.peek() != null) {
                    ResultEntity res=queue.peek();
                    if (res.getSessions() <= objectEntity.getSessions()) {
                        queue.poll();
                        map.remove(res.getCommon_client_ip());
                        ResultEntity resultEntity =new ResultEntity();
                        resultEntity.setSessions(objectEntity.getSessions());
                        resultEntity.setCommon_client_ip(objectEntity.getCommon_client_ip());
                   //     resultEntity.setCommon_app_label(objectEntity.getCommon_app_label());
                   //     resultEntity.setCommon_recv_time(objectEntity.getCommon_recv_time());
                        queue.add(resultEntity);
                        map.put(objectEntity.getCommon_client_ip(),objectEntity.getSessions());
                    }
                }
            }
        }
        else{

            PriorityQueue<ResultEntity> que = new PriorityQueue<>();
            Map<String,Long> map = new HashMap<>();

            ResultEntity resultEntity =new ResultEntity();
            resultEntity.setSessions(objectEntity.getSessions());
            resultEntity.setCommon_client_ip(objectEntity.getCommon_client_ip());
           // resultEntity.setCommon_app_label(objectEntity.getCommon_app_label());
           // resultEntity.setCommon_recv_time(objectEntity.getCommon_recv_time());
            que.add(resultEntity);
            map.put(objectEntity.getCommon_client_ip(),objectEntity.getSessions());
            resultMap.put(objectEntity.getCommon_app_label(),que);
            countMap.put(objectEntity.getCommon_app_label(),map);

        }
        //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
        context.timerService().registerEventTimeTimer(objectEntity.getCommon_recv_time() + 1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String,String,Long>> out) throws Exception {

        if(resultMap.size()>0) {
/*
            List<Map.Entry<String, Long>> objectlist = new ArrayList<Map.Entry<String, Long>>(resultMap.entrySet());


            Collections.sort(objectlist, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return Math.toIntExact((o2.getValue() - o1.getValue()));
                }
            });
*/

            for (Map.Entry<String, PriorityQueue<ResultEntity>> entry : resultMap.entrySet()) {
                PriorityQueue<ResultEntity> queue= entry.getValue();
                String jsonStr = JSONObject.toJSONString(queue);
                Tuple3<String,String,Long> emit= new Tuple3<>(entry.getKey(),jsonStr,ctx.timestamp());
                out.collect(emit);
            }
            resultMap.clear();
            countMap.clear();
        }

      /*  if(queue.size()>0) {
            int i = 0;
            StringBuilder sb = new StringBuilder();
            CipEntity ce = new CipEntity();
            for (ObjectEntity obj : queue) {

                if (i == 0) {
                    ce.setCommon_app_label(obj.getCommon_app_label());
                    ce.setCommon_recv_time(obj.getCommon_recv_time());

                }
                sb.append(obj.getCommon_client_ip());
                sb.append(",");
                i++;
                if (i >= topSize) {
                    break;
                }
            }
            ce.setCommon_client_list(sb.toString());
            out.collect(ce);
            queue.clear();
        }*/

    }
}