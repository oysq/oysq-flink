package com.oysq.flink.datastream.source.access;

import com.oysq.flink.datastream.transformation.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Random;

/**
 * 自定义数据源 - 单并行度
 */
public class AccessSource implements SourceFunction<Access> {

    private boolean run = true;

    String[] urls = new String[]{"baidu.com", "ali.com", "abc.com"};

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        Random random = new Random();
        while(run) {
            Access access = new Access();
            access.setUrl(urls[random.nextInt(3)]);
            access.setNum(random.nextLong());
            access.setDateStr(new Date().toString());
            ctx.collect(access);

            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {

        System.out.println("=== cancel ===");

        this.run = false;
    }
}
