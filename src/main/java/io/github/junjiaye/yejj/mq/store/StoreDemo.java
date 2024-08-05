package io.github.junjiaye.yejj.mq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import io.github.junjiaye.yejj.mq.model.YeJJMessage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Scanner;

/**
 * @program: yejjmq
 * @ClassName: StoreDemo
 * @description:
 * @author: yejj
 * @create: 2024-08-05 14:30
 */
public class StoreDemo {
    public static void main(String[] args) throws IOException {
        String content = """
                    this is a good file.
                    this is a new line for strore.
                """;
        System.out.println("len = " + content.getBytes(StandardCharsets.UTF_8).length);
        File file = new File("test.bat");
        if (!file.exists()){
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        //内存映射
        try (FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ,StandardOpenOption.WRITE)){
            //创建1k长度的内存缓冲区
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,1024);
            for (int i = 0; i < 10; i++) {
                System.out.println( i + "-->" +mappedByteBuffer.position());
                YeJJMessage<String> message = YeJJMessage.create(i + " :" +content,null);
                String msg = JSON.toJSONString(message);
                Indexer.addEntry("test",mappedByteBuffer.position(),msg.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
            }
            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()){
                String line = sc.nextLine();
                if (line.equals("q")) break;
                System.out.println(" in = " + line);
                int id = Integer.parseInt(line);
                Indexer.Entry entr = Indexer.getEntry("test", id);
                readOnlyBuffer.position(entr.getOffset());
                byte[] bytes = new byte[entr.getLength()];
                readOnlyBuffer.get(bytes,0,entr.getLength());
                String s = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("read only===>" + s);
                YeJJMessage<String> jsonObject = JSON.parseObject(s,new TypeReference<YeJJMessage<String>>(){
                });
                System.out.println(jsonObject.getBody());
            }
        }
    }
}
