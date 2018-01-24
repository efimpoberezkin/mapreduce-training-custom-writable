package com.epam.training.bigdata.mapred.second;

import com.epam.training.bigdata.mapred.second.writable.FloatIntPairWritable;
import com.epam.training.bigdata.mapred.second.writable.IntPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BytesByIPTest {

    private MapDriver<LongWritable, Text, Text, IntPairWritable> mapDriver;
    private ReduceDriver<Text, IntPairWritable, Text, FloatIntPairWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntPairWritable, Text, FloatIntPairWritable> mapReduceDriver;

    @Before
    public void setUp() {
        BytesByIPMapper mapper = new BytesByIPMapper();
        BytesByIPReducer reducer = new BytesByIPReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withInput(new LongWritable(), new Text("ip43 - - [24/Apr/2011:06:35:10 -0400] \"GET /next HTTP/1.1\" 301 312 \"-\" \"Java/1.6.0_04\""));
        mapDriver.withInput(new LongWritable(), new Text("ip108 - - [24/Apr/2011:10:07:17 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"-\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.1; computerbild; computerbild)\""));
        mapDriver.withInput(new LongWritable(), new Text("ip13 - - [24/Apr/2011:10:29:50 -0400] \"GET / HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""));
        mapDriver.withOutput(new Text("ip1"), new IntPairWritable(40028, 1));
        mapDriver.withOutput(new Text("ip43"), new IntPairWritable(312, 1));
        mapDriver.withOutput(new Text("ip108"), new IntPairWritable(0, 1));
        mapDriver.withOutput(new Text("ip13"), new IntPairWritable(0, 1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntPairWritable> ip1Values = new ArrayList<>();
        ip1Values.add(new IntPairWritable(300, 3));
        ip1Values.add(new IntPairWritable(50, 1));
        reduceDriver.withInput(new Text("ip1"), ip1Values);
        reduceDriver.withOutput(new Text("ip1"), new FloatIntPairWritable((float) 350 / 4, + 350));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip2 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 517 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:06:35:10 -0400] \"GET /next HTTP/1.1\" 301 312 \"-\" \"Java/1.6.0_04\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip2 - - [24/Apr/2011:10:07:17 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"-\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.1; computerbild; computerbild)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:10:29:50 -0400] \"GET / HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""));
        mapReduceDriver.withOutput(new Text("ip1"), new FloatIntPairWritable((float) (40028 + 312) / 3, 40028 + 312));
        mapReduceDriver.withOutput(new Text("ip2"), new FloatIntPairWritable((float) 517 / 2, + 517));

        mapReduceDriver.runTest();
    }
}
