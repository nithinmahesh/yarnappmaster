package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by nithinm on 2/18/2016.
 */
public class AppMasterService extends Configured {
    private static final Log LOG = LogFactory.getLog(AppMaster.class);
    private ApplicationAttemptId appAttemptID;
    private Configuration conf;
    private ContainerId containerId;
    private AMRMClientAsync amRmClient;
    String trackingUrl;

    public AppMasterService()
    {
        conf = getConf();
    }

    private void getAttemptId() {
        Map<String, String> envs = System.getenv();
        String containerIdString =
                envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        containerId = ConverterUtils.toContainerId(containerIdString);
        LOG.info("ContainerId = " + containerId);
        appAttemptID = containerId.getApplicationAttemptId();
        LOG.info("ApplicationAttemptId = " + appAttemptID);
    }

    private boolean registerAM() {
        amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, new YarnAppCallbackHandler());
        conf = new Configuration();
        amRmClient.init(conf);
        amRmClient.start();

        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        trackingUrl = "someTrackingUrl";

        try {
            RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(hostname, 8888, trackingUrl);
            LOG.info("Register AM, response : " + response.toString());
        }
        catch (Exception e) {
            LOG.info ("RegisterAM hit exception: " + e.getStackTrace());
            return false;
        }

        return true;
    }

    private void runProcess() {
        try {
            String command = "cmd /C dir > c:\\out.txt";
            Process myProcess = Runtime.getRuntime().exec(command);
            LOG.info("Process successfully started");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void finishAM() {
        FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
        String appMsg = "I'm done";
        try {
            amRmClient.unregisterApplicationMaster(status, appMsg, trackingUrl);
            LOG.info("AppService finished");
        }
        catch (Exception e) {
            LOG.info ("UnRegisterAM hit exception: " + e.getStackTrace());
        }

        amRmClient.stop();
    }

    public void start() {
        getAttemptId();
        registerAM();
        runProcess();
    }

    public void shutdown() {
        finishAM();
    }
}
