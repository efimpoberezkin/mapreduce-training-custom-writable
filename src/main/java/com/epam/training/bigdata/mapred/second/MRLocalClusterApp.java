package com.epam.training.bigdata.mapred.second;

import com.github.sakserv.minicluster.impl.MRLocalCluster;
import org.apache.hadoop.conf.Configuration;

public class MRLocalClusterApp {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_HOME", "C:/hadoop-mini-clusters");

        MRLocalCluster mrLocalCluster = new MRLocalCluster.Builder()
                .setNumNodeManagers(1)
                .setJobHistoryAddress("localhost:37005")
                .setResourceManagerAddress("localhost:37001")
                .setResourceManagerHostname("localhost")
                .setResourceManagerSchedulerAddress("localhost:37002")
                .setResourceManagerResourceTrackerAddress("localhost:37003")
                .setResourceManagerWebappAddress("localhost:37004")
                .setUseInJvmContainerExecutor(false)
                .setConfig(new Configuration())
                .build();

        mrLocalCluster.start();
    }
}
