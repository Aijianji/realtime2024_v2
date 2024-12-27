package com.yiqun.deploy;

import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;

import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterInformationRetriever;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;

/**
 * @Author yiqun.shi
 * @Date 2024/12/26 20:53
 * @description: 这个代码是可以将jar包在本地上传部署至yarn的，就不用再把jar包放到终端里,然后去flink用./bin/...那个命令运行部署了。所以这些代码和次项目无关
 */
public class FlinkJobSubmitToYarnApplicationModel {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobSubmitToYarnApplicationModel.class.getName());
    private static final String FLINK_SUBMIT_USER = ConfigUtils.getString("flink.submit.user");
    private static final String FLINK_COMMON_CONF_DIR = ConfigUtils.getString("flink.conf.configurationDirectory");
    private static final String FLINK_CLUSTER_LIBS_DIR = ConfigUtils.getString("flink.cluster.libs");
    public static void main(String[] args) {

        CommonUtils.printCheckPropEnv(false,FLINK_SUBMIT_USER,FLINK_COMMON_CONF_DIR,FLINK_CLUSTER_LIBS_DIR);

        // RestFul
        SubFlinkTask(
                FLINK_SUBMIT_USER,
                FLINK_COMMON_CONF_DIR,
                FLINK_CLUSTER_LIBS_DIR,
                "hdfs://cdh01:8020/flink-jars/",
                "testtest",
                ""
        );

    }

    public static void SubFlinkTask(String submitUser,
                                    String configurationDirectory,
                                    String flinkClusterLibDir,
                                    String userJarPath,
                                    String jobName,
                                    String fullClassName){

        System.setProperty("HADOOP_USER_NAME",submitUser);
        String flinkDistJar = "hdfs://cdh01:8020/flink-dist/lib/flink-yarn-1.17.1.jar";
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        YarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever
                .create(yarnClient);
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(
                configurationDirectory);

        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        flinkConfiguration.set(
                PipelineOptions.JARS,
                Collections.singletonList(userJarPath)
        );

        Path remoteLib = new Path(flinkClusterLibDir);
        flinkConfiguration.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                Collections.singletonList(remoteLib.toString())
        );

        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar
        );

        // 设置为application模式
        flinkConfiguration.set(
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName()
        );

        flinkConfiguration.set(
                YarnConfigOptions.APPLICATION_QUEUE,
                "default"
        );
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, jobName);
        YarnLogConfigUtil.setLogConfigFileInConfig(flinkConfiguration, configurationDirectory);

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();

        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[] {"test"}, fullClassName);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                true);
        try {
            ClusterClientProvider<ApplicationId> clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    appConfig);

            ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();

            ApplicationId applicationId = clusterClient.getClusterId();
            String webInterfaceURL = clusterClient.getWebInterfaceURL();
            LOG.info("applicationId is {}", applicationId);
            LOG.info("webInterfaceURL is {}", webInterfaceURL);

        } catch (Exception e){
            LOG.error(e.getMessage(), e);
        }
    }
}