package io.github.junjiaye.yejj.mq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.junjiaye.yejj.mq.model.YeJJMessage;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @program: yejjmq
 * @ClassName: Store
 * @description:
 * @author: yejj
 * @create: 2024-08-05 15:39
 */
public class Store {

    public static final int SIZE = 1024 * 1024 * 100;
    private String topic;
    MappedByteBuffer mappedByteBuffer = null;

    public Store(String topic){
        this.topic = topic;
    }
    @SneakyThrows
    public  void init() {
        File file = new File(topic + ".dat");
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        //内存映射
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        //创建1k长度的内存缓冲区
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, SIZE);
    }

     public int write(YeJJMessage<String> message){
         String msg = JSON.toJSONString(message);
         int position = mappedByteBuffer.position();
         Indexer.addEntry(topic, position,msg.getBytes(StandardCharsets.UTF_8).length);
         mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
         return position;
     }

     public YeJJMessage<String> read(int offset){
         ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
         Indexer.Entry entr = Indexer.getEntry(topic, offset);
         readOnlyBuffer.position(entr.getOffset());
         byte[] bytes = new byte[entr.getLength()];
         readOnlyBuffer.get(bytes,0,entr.getLength());
         String msgJson = new String(bytes, StandardCharsets.UTF_8);
         System.out.println("read only===>" + msgJson);
         YeJJMessage<String> message = JSON.parseObject(msgJson,new TypeReference<YeJJMessage<String>>(){
         });
         return message;
     }

     public int pos(){
        return mappedByteBuffer.position();
     }
}
