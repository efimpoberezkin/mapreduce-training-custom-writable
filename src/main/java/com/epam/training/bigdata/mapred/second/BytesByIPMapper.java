package com.epam.training.bigdata.mapred.second;

import com.epam.training.bigdata.mapred.second.writable.IntPairWritable;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Takes a log entry and maps it to IP and pair of bytes sum and amount of requests from IP.
 */
public class BytesByIPMapper extends Mapper<LongWritable, Text, Text, IntPairWritable> {

    private Text ip = new Text();
    private IntPairWritable sumAndCount = new IntPairWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String logEntry = value.toString();
        ip.set(getIpFromLogEntry(logEntry));
        sumAndCount.set(getBytesFromLogEntry(logEntry), 1);
        context.write(ip, sumAndCount);

        UserAgent userAgent = new UserAgent(getUserAgentFromLogEntry(logEntry));
        String browser = userAgent.getBrowser().getName();
        context.getCounter("Browsers", browser).increment(1);
    }

    private int getBytesFromLogEntry(String logEntry) {
        int secondQuotationMarkIndex = StringUtils.ordinalIndexOf(logEntry, "\"", 2);
        String bytesSubstring = logEntry.substring(secondQuotationMarkIndex + 6); // substring of log entry starting with bytes, offset 6 is for 2 spaces and a response code
        String bytesStr = bytesSubstring.substring(0, bytesSubstring.indexOf(' ')); // bytes value as String
        return "-".equals(bytesStr) ? 0 : Integer.valueOf(bytesStr);
    }

    private String getIpFromLogEntry(String logEntry) {
        return logEntry.substring(0, logEntry.indexOf(' '));
    }

    private String getUserAgentFromLogEntry(String logEntry) {
        int fifthQuotationMarkIndex = StringUtils.ordinalIndexOf(logEntry, "\"", 5);
        return logEntry.substring(fifthQuotationMarkIndex + 1, logEntry.length() - 1);
    }
}
