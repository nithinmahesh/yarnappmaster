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
 * Class that implements all functions of a Yarn Application Master
 */
public class AppMasterService extends Configured {
    private static final Log LOG = LogFactory.getLog(AppMasterService.class);
    private static String HadoopHome = "HADOOP_HOME";
    private static int AMHeartbeatIntervalMs = 1000;
    private ApplicationAttemptId appAttemptID;
    private Configuration conf;
    private ContainerId containerId;
    private AMRMClientAsync amRmClient;
    String trackingUrl;
    private String metastorePort = "";
    private Process beelineProcess;
    private Process hs2Process;
    private Process metastoreProcess;
    private String logDir;
    private String workingDir;
    private String service;
    private WebApp webApp;

    public AppMasterService(String serviceName, int minport, int maxport)
    {
        conf = new Configuration();
        beelineProcess = null;
        hs2Process = null;
        metastoreProcess = null;
        trackingUrl = null;
        service = serviceName.toLowerCase();
        int midport = (minport + maxport) / 2;
        webApp = new WebApp(minport, midport, this);

        if (service.equalsIgnoreCase(Service.METASTORE.toString())) {
            int availablePort = PortAvailabilityDetector.getAvailablePortInRange(midport + 1, maxport);
            if (availablePort != 0) {
                metastorePort = String.valueOf(availablePort);
            }
        }
    }

    public enum Service {
        METASTORE,
        HIVESERVER2,
        BEELINE
    }

    private boolean setupApplicationVariables() throws IllegalArgumentException{
        Map<String, String> envs = System.getenv();
        String containerIdString = envs.get(
                ApplicationConstants.Environment.CONTAINER_ID.toString());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }

        containerId = ConverterUtils.toContainerId(containerIdString);
        appAttemptID = containerId.getApplicationAttemptId();
        logDir = System.getenv(ApplicationConstants.Environment.LOG_DIRS.toString());
        workingDir = System.getenv(ApplicationConstants.Environment.PWD.toString());

        LOG.info("ContainerId = " + containerId);
        LOG.info("ApplicationAttemptId = " + appAttemptID);
        LOG.info("LogDir = " + logDir);
        LOG.info("WorkingDir = " + workingDir);

        return true;
    }

    private boolean registerAM() {
        amRmClient = AMRMClientAsync.createAMRMClientAsync(
                AMHeartbeatIntervalMs, new YarnAppCallbackHandler());
        amRmClient.init(conf);
        amRmClient.start();
        webApp.startServer();

        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        trackingUrl = webApp.getServerUrl();

        try {
            RegisterApplicationMasterResponse response =
                    amRmClient.registerApplicationMaster(hostname, -1, trackingUrl);
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

    private Map<String, String> setSystemEnvVariables() {
        Map<String, String> environment = new HashMap<String, String>();

        String[] whitelist = conf.get(
                YarnConfiguration.NM_ENV_WHITELIST,
                YarnConfiguration.DEFAULT_NM_ENV_WHITELIST).split(",");

        for (String whitelistEnvVariable : whitelist) {
            putEnvIfAbsent(environment, whitelistEnvVariable.trim());
        }

        // Hive needs HADOOP_HOME to be set
        //
        if (System.getenv(HadoopHome) == null) {
            environment.put(HadoopHome,System.getenv(
                    ApplicationConstants.Environment.HADOOP_COMMON_HOME.toString()));
        }

        return environment;
    }

    private String generateStartScript(Service service) {
        Map<String, String> environment = setSystemEnvVariables();

        // Setup other Hive variables
        //
        environment.put("HIVE_OPTS",
                " -hiveconf hive.querylog.location=" + logDir
                        + " -hiveconf hive.log.dir=" + logDir);

        // Create a script start<servicename>.cmd that sets JAVA_HOME and HADOOP_HOME and
        // starts all the services - metastore, HS2 and then beeline
        //
        String fileContent = "@echo on\r\n";
        for (Map.Entry<String, String> env : environment.entrySet()) {
            fileContent += "set " + env.getKey() + "=" + env.getValue() + "\r\n";
        }

        fileContent += "cd HS2\r\n";
        fileContent += "cd hive*\r\n";
        fileContent += "cd conf\r\n";
        fileContent += "rename hive-site.xml hive-siteTemplate.xml\r\n";
        fileContent += "copy /Y ..\\..\\..\\hive-site.xml hive-site.xml\r\n";
        fileContent += "cd ..\\bin\r\n";

        if (service == Service.BEELINE) {
            fileContent += service.toString().toLowerCase() +
                    " -u jdbc:hive2://localhost:10001/;transportMode=http;hive.server2.servermode=http " +
                    "-n tester -p password " + "" +
                    "-f ../../../script.q > "
                    + logDir
                    + "/beelineoutput.txt";
        } else {
            fileContent += "hive --service " + service.toString().toLowerCase();
            if (service == Service.METASTORE) {
                fileContent += " -p " + metastorePort;
            }
        }

        fileContent += "\r\n";

        byte data[] = fileContent.getBytes();
        String scriptName = "start" + service.toString().toLowerCase() + ".cmd";
        Path p = Paths.get("./"+ scriptName);

        try {
            OutputStream out = new BufferedOutputStream(Files.newOutputStream(p, CREATE, APPEND));
            out.write(data, 0, data.length);
            out.flush();
            out.close();
        } catch (IOException x) {
            System.err.println(x);
        }

        return scriptName;
    }

    private Process startProcess(String scriptName) throws IOException {
        String command = "cmd /C " + scriptName +
                " > " + logDir + "/" + scriptName + "log.txt";
        return Runtime.getRuntime().exec(command);
    }

    // Do the actual work to start application master logic
    //
    private boolean doStartupWork() {
        try {
            if (service.equalsIgnoreCase(Service.METASTORE.toString())) {
                if (metastorePort == "") {
                    throw new Exception("No ports available in the given range.");
                }

                metastoreProcess = startProcess(generateStartScript(Service.METASTORE));
                LOG.info("Metastore Process " + metastoreProcess.toString() +
                        " successfully started in port " + metastorePort + "\n");
            } else {
                hs2Process = startProcess(generateStartScript(Service.HIVESERVER2));
                LOG.info("Hiveserver2 Process " + hs2Process.toString() +
                        " successfully started in port 10001\n");
                ThreadUtil.sleepAtLeastIgnoreInterrupts(120000);

                beelineProcess = startProcess(generateStartScript(Service.BEELINE));
                LOG.info("Beeline Process " + beelineProcess.toString() +
                        " successfully started.\n");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            shutdown();
            return  false;
        }

        return true;
    }

    private boolean finishAM() {
        boolean result = false;
        webApp.stopServer();
        FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
        String appMsg = "AppService completed successfully";
        try {
            amRmClient.unregisterApplicationMaster(status, appMsg, trackingUrl);
            LOG.info("AppService finished");
            result = true;
        }
        catch (Exception e) {
            LOG.info ("UnRegisterAM hit exception: " + e.getStackTrace());
        }

        amRmClient.stop();

        return result;
    }

    public boolean start() {
        boolean result = true;
        result &= setupApplicationVariables();
        result &= registerAM();
        result &= doStartupWork();

        return result;
    }

    public void waitForCompletion() {
        try {
            // If not a beeline process, shutdown will be called from the higher layer
            //
            if (beelineProcess != null) {
                beelineProcess.waitFor();
                shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
            shutdown();
        }
    }

    public boolean shutdown() {
        if (hs2Process != null) {
            hs2Process.destroy();
        }
        if (metastoreProcess != null) {
            metastoreProcess.destroy();
        }

        return finishAM();
    }

    public String getMetastoreUrl() {
        if (service.equalsIgnoreCase(Service.METASTORE.toString())) {
            String hostname = "";
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            return "thrift://" + hostname + ":" + metastorePort;
        }

        return "";
    }
}
