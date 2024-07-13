package io.github.junjiaye.yejj.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @program: yejjmq
 * @ClassName: Result
 * @description: 返回结果
 * @author: yejj
 * @create: 2024-07-13 16:23
 */
@Data
@AllArgsConstructor
public class Result<T> {
    private int code; //1==成功  0 ==失败
    private T data;

    public static Result<String> ok(){
       return new Result(1,"OK");
    }

    public static Result<String> ok(String msg){
        return new Result(1,msg);
    }

    public static Result<YeJJMessage<?>> msg(String msg) {
        return new Result<>(1,YeJJMessage.create(msg,null));
    }
    public static Result<YeJJMessage<?>> msg(YeJJMessage<?> msg) {
        return new Result<>(1,msg);
    }
}
