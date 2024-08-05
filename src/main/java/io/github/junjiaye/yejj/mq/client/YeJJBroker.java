package io.github.junjiaye.yejj.mq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.junjiaye.yejj.mq.model.Result;
import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;



/**
 * @program: yejjmq
 * @ClassName: YeJJBroker
 * @description:
 * @author: yejj
 * @create: 2024-07-13 08:20
 */
public class YeJJBroker {

    public static YeJJBroker Default = new YeJJBroker();
    private static String brokerUrl = "http://localhost:8765/yejjmq";

    static {
        //init();
    }

    private static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(()->{

        },100,100);
    }

    public  YeJJProducer createProducer(){
        return new YeJJProducer(this);
    }
    public  YeJJConsumer<?> createConsumer(String topic){
        YeJJConsumer<?> consumer = new YeJJConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public void sub(String topic, String cid) {
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + cid,new TypeReference<Result<String>>(){});
        System.out.println("=====>>sub result : " + result);
    }

    public <T> YeJJMessage<T> recv(String topic, String id) {
        Result<YeJJMessage<String>> result = HttpUtils.httpGet( brokerUrl + "/recv?t=" + topic  + "&cid=" + id,new TypeReference<Result<YeJJMessage<String>>>(){});
        System.out.println("=====>>recv result : " + result);

        return (YeJJMessage<T>) result.getData();
    }

    public boolean send(String topic,YeJJMessage message) {
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message), brokerUrl + "/send?t=" + topic,new TypeReference<Result<String>>(){});
        System.out.println("=====>>send result : " + result);
        return result.getCode() == 1;
    }

    public void unsub(String topic, String cid) {
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + cid,new TypeReference<Result<String>>(){});
        System.out.println("=====>>unsub result : " + result);
    }
    public boolean ack(String topic, String cid,int offset) {
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + cid+ "&offset=" + offset,new TypeReference<Result<String>>(){});
        System.out.println("=====>>ack result : " + result);
        return result.getCode() == 1;
    }
    private MultiValueMap<String,YeJJListener<?>> listeners = new LinkedMultiValueMap();
    public <T> void addListener(String topic,YeJJListener<T> listener) {
        listeners.add(topic,listener);
    }


}
