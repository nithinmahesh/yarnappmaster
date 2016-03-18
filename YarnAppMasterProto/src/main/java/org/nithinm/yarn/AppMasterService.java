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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static java.nio.file.StandardOpenOption.*;
import java.nio.file.*;
import java.io.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
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
    private String port = "2000";
    // private String hivelocation = "hive-1.2.1.0.0.0.0-0separateMetastore";
    private String hivelocation = "hive-1.2.1.2.3.3.1-5";
    private Process beelineProcess;
    private Process hs2Process;
    private Process metastoreProcess;

    public AppMasterService()
    {
        conf = getConf();
    }

    private enum Service {
        METASTORE,
        HIVESERVER2,
        BEELINE
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

    private static void putEnvIfNotNull(
            Map<String, String> environment, String variable, String value) {
        if (value != null) {
            environment.put(variable, value);
        }
    }

    private static void putEnvIfAbsent(
            Map<String, String> environment, String variable) {
        if (environment.get(variable) == null) {
            putEnvIfNotNull(environment, variable, System.getenv(variable));
        }
    }

    private void generateStartScript(Service service) {
        String[] whitelist = conf.get(YarnConfiguration.NM_ENV_WHITELIST, YarnConfiguration.DEFAULT_NM_ENV_WHITELIST).split(",");

        Map<String, String> environment = new HashMap<String, String>();

        for (String whitelistEnvVariable : whitelist) {
            putEnvIfAbsent(environment, whitelistEnvVariable.trim());
        }

        environment.put("HADOOP_HOME", "%" + ApplicationConstants.Environment.HADOOP_COMMON_HOME + "%");

        // Create a script start<servicename>.cmd that sets JAVA_HOME and HADOOP_HOME and
        // starts all the services - metastore, HS2 and then beeline
        //
        String fileContent = "@echo on\r\n";
        for (Map.Entry<String, String> env : environment.entrySet()) {
            fileContent += "set " + env.getKey() + "=" + env.getValue() + "\r\n";
        }
        fileContent += "cd HS2\\" + hivelocation + "\\conf\r\n";

        fileContent += "rename hive-site.xml hive-siteTemplate.xml\r\n";

        fileContent += "copy ..\\..\\..\\hive-site.xml hive-site.xml\r\n";

        fileContent += "cd ..\\bin\r\n";

        if (service == Service.BEELINE) {
            fileContent += service.toString().toLowerCase() +
                    " -u jdbc:hive2://localhost:10001/;transportMode=http;hive.server2.servermode=http " +
                    "-n tester -p password " + "" +
                    "-f ../../../script.q > ../../../beelineoutput.txt";
        } else {
            fileContent += "hive --service " + service.toString().toLowerCase();
            if (service == Service.METASTORE) {
                fileContent += " -p " + port;
            }
        }

        fileContent += "\r\n";

        byte data[] = fileContent.getBytes();
        Path p = Paths.get("./start" + service.toString().toLowerCase() + ".cmd");

        try {
            OutputStream out = new BufferedOutputStream(Files.newOutputStream(p, CREATE, APPEND));
            out.write(data, 0, data.length);
            out.flush();
            out.close();
        } catch (IOException x) {
            System.err.println(x);
        }

    }

    private void copyOutput() {
        /*
                            "FOR /F %%a IN ('POWERSHELL -COMMAND \"$([guid]::NewGuid().ToString())\"') DO ( SET NEWGUID=%%a )\r\n" +
                            "hadoop fs -mkdir adl://nithinadls.caboaccountdogfood.net/output\r\n" +
                            "hadoop fs -mkdir adl://nithinadls.caboaccountdogfood.net/output/%NEWGUID%\r\n" +
                            "hadoop fs -copyFromLocal -f beelineoutput.txt adl://nithinadls.caboaccountdogfood.net/output/%NEWGUID%\r\n" +
                            "hadoop fs -copyFromLocal -f log.txt adl://nithinadls.caboaccountdogfood.net/output/%NEWGUID%";
*/
    }

    private void runProcess() {
        try {
            Service service = Service.METASTORE;
            generateStartScript(service);
            String metastoreCommand = "cmd /C start" + service.toString().toLowerCase() + ".cmd" +
                    " > C:/start" + service.toString().toLowerCase() + "log.txt";
            metastoreProcess = Runtime.getRuntime().exec(metastoreCommand);
            LOG.info("Metastore Process successfully started in port " + port + "\n");
            ThreadUtil.sleepAtLeastIgnoreInterrupts(60000);

            service = Service.HIVESERVER2;
            generateStartScript(service);
            String hs2Command = "cmd /C start" + service.toString().toLowerCase() + ".cmd" +
                    " > C:/start" + service.toString().toLowerCase() + "log.txt";
            hs2Process = Runtime.getRuntime().exec(hs2Command);
            LOG.info("Hiveserver2 Process successfully started in port 10001\n");
            ThreadUtil.sleepAtLeastIgnoreInterrupts(120000);

            service = Service.BEELINE;
            generateStartScript(service);
            String beelineCommand = "cmd /C start" + service.toString().toLowerCase() + ".cmd" +
                    " > C:/start" + service.toString().toLowerCase() + "log.txt";
            beelineProcess = Runtime.getRuntime().exec(beelineCommand);
            LOG.info("Beeline Process successfully started.\n");
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

    public void waitForCompletion() {
        try {
            beelineProcess.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }

        copyOutput();

        // hs2Process.destroyForcibly();
        // metastoreProcess.destroyForcibly();
    }

    public void shutdown() {
        finishAM();
    }
}
