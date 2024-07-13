package io.github.junjiaye.yejj.mq.server;

import io.github.junjiaye.yejj.mq.model.Result;
import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: yejjmq
 * @ClassName: MQServer
 * @description:
 * @author: yejj
 * @create: 2024-07-13 10:51
 */
@RestController
@RequestMapping("/yejjmq")
public class MQServer {
    //send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic, @RequestParam("cid") String comsumerId,
                               @RequestBody YeJJMessage<String> message){
        //
        return Result.ok(String.valueOf(MessageQueue.send(topic,comsumerId,message)));
    }
    //recv
    @RequestMapping("/recv")
    public Result<YeJJMessage<?>> recv(@RequestParam("t") String topic, @RequestParam("cid") String comsumerId,
                       @RequestBody YeJJMessage<String> message){
        //
        return Result.msg(MessageQueue.recv(topic,comsumerId));
    }
    //ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic, @RequestParam("cid") String comsumerId,
                              @RequestParam("offset") Integer offset){

        return Result.ok(String.valueOf(MessageQueue.ack(topic,comsumerId,offset)));
    }
    //sub
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,@RequestParam("cid") String comsumerId){
        MessageQueue.sub(new MessageSubscription(topic,comsumerId,-1));
        return Result.ok();
    }
    //unsub
    @RequestMapping("/ubsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,@RequestParam("cid") String comsumerId){
        MessageQueue.unsub(new MessageSubscription(topic,comsumerId,-1));
        return Result.ok();
    }
}
