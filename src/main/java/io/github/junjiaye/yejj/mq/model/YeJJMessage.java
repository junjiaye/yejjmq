package io.github.junjiaye.yejj.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: yejjmq
 * @ClassName: YeJJMessage
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:16
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class YeJJMessage<T> {
    //private String topic;
    static AtomicLong idgen = new AtomicLong(0);
    private Long id;
    private T body;
    //系统属性
    private Map<String,String> headers = new HashMap<>();
    //业务属性
    //private Map<String,String> properties;

    public static long nextId(){
        return idgen.getAndIncrement();
    }

    public static YeJJMessage create(String body,Map<String,String> headers){
        return new YeJJMessage<>(nextId(),body,headers);
    }
}
