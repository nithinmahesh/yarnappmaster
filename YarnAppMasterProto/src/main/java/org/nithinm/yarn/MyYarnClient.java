package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.sql.rowset.spi.XmlWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by nithinm on 2/18/2016.
 */
public class MyYarnClient extends Configured {
    private String appName = "MyTestApp";
    private String storageAccount = "wasb://nithinmhdi2@nithinwasb.blob.core.windows.net/";
    private static final Log LOG = LogFactory.getLog(MyYarnClient.class);
    private Configuration conf;
    private int amMemory = 2048;
    private int amVCores = 4;

    public void startJob() {
        ApplicationReport report = startMetastore();
    }

    public Map<String, LocalResource> addLocalResources() throws  Exception{
        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");

        FileSystem fs = FileSystem.get(conf);

        String amJarUri = storageAccount + "jars/YarnAppMasterProto-1.0-SNAPSHOT.jar";
        Path jarPath = fs.makeQualified(new Path(amJarUri)); // <- known path to jar file
        FileStatus jarStatus = fs.getFileStatus(jarPath);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        // Set the type of resource - file or archive
        // archives are untarred at the destination by the framework
        amJarRsrc.setType(LocalResourceType.FILE);
        // Set visibility of the resource
        // Setting to most private option i.e. this file will only
        // be visible to this instance of the running application
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        // Set the location of resource to be copied over into the
        // working directory
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        // Set timestamp and length of file so that the framework
        // can do basic sanity checks for the local resource
        // after it has been copied over to ensure it is the same
        // resource the client intended to use with the application
        amJarRsrc.setTimestamp(jarStatus.getModificationTime());
        amJarRsrc.setSize(jarStatus.getLen());
        // The framework will create a symlink called AppMaster.jar in the
        // working directory that will be linked back to the actual file.
        // The ApplicationMaster, if needs to reference the jar file, would
        // need to use the symlink filename.
        localResources.put("AppMaster", amJarRsrc);

        String HS2JarUri = storageAccount + "jars/hive-1.2.1.2.3.3.1-5.zip";
        Path hs2jarPath = fs.makeQualified(new Path(HS2JarUri)); // <- known path to jar file
        FileStatus hs2jarStatus = fs.getFileStatus(hs2jarPath);
        LocalResource hs2JarRsrc = Records.newRecord(LocalResource.class);
        // Set the type of resource - file or archive
        // archives are untarred at the destination by the framework
        hs2JarRsrc.setType(LocalResourceType.ARCHIVE);
        // Set visibility of the resource
        // Setting to most private option i.e. this file will only
        // be visible to this instance of the running application
        hs2JarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        // Set the location of resource to be copied over into the
        // working directory
        hs2JarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(hs2jarPath));
        // Set timestamp and length of file so that the framework
        // can do basic sanity checks for the local resource
        // after it has been copied over to ensure it is the same
        // resource the client intended to use with the application
        hs2JarRsrc.setTimestamp(hs2jarStatus.getModificationTime());
        hs2JarRsrc.setSize(hs2jarStatus.getLen());
        // The framework will create a symlink called AppMaster.jar in the
        // working directory that will be linked back to the actual file.
        // The ApplicationMaster, if needs to reference the jar file, would
        // need to use the symlink filename.
        localResources.put("HS2", hs2JarRsrc);

        return localResources;
    }

    public Map<String, String> addEnvVariables() {
        Map<String, String> env = new HashMap<String, String>();

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        env.put("CLASSPATH", classPathEnv.toString());

        return env;
    }

    public ApplicationReport startMetastore() {
        try {
            YarnClient yarnClient = YarnClient.createYarnClient();
            conf = new Configuration();
            LOG.info("Conf:" + conf.toString());
            yarnClient.init(conf);
            yarnClient.start();

            YarnClientApplication app = yarnClient.createApplication();
            GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

            // set the application submission context
            ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
            ApplicationId appId = appContext.getApplicationId();

            appContext.setKeepContainersAcrossApplicationAttempts(false);
            appContext.setApplicationName(appName);

            // set local resources for the application master
            // local files or archives as needed
            // In this scenario, the jar file for the application master is part of the local resources
            Map<String, LocalResource> localResources = addLocalResources();
            FileSystem fs = FileSystem.get(conf);

            String execScript = storageAccount + "jars/hive-site.xml";
            Path scriptPath = fs.makeQualified(new Path(execScript)); // <- known path to jar file
            FileStatus scriptStatus = fs.getFileStatus(scriptPath);
            LocalResource scriptRsrc = Records.newRecord(LocalResource.class);
            // Set the type of resource - file or archive
            // archives are untarred at the destination by the framework
            scriptRsrc.setType(LocalResourceType.FILE);
            // Set visibility of the resource
            // Setting to most private option i.e. this file will only
            // be visible to this instance of the running application
            scriptRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            // Set the location of resource to be copied over into the
            // working directory
            scriptRsrc.setResource(ConverterUtils.getYarnUrlFromPath(scriptPath));
            // Set timestamp and length of file so that the framework
            // can do basic sanity checks for the local resource
            // after it has been copied over to ensure it is the same
            // resource the client intended to use with the application
            scriptRsrc.setTimestamp(scriptStatus.getModificationTime());
            scriptRsrc.setSize(scriptStatus.getLen());
            // The framework will create a symlink called AppMaster.jar in the
            // working directory that will be linked back to the actual file.
            // The ApplicationMaster, if needs to reference the jar file, would
            // need to use the symlink filename.
            localResources.put("hive-site.xml", scriptRsrc);

            // Set the env variables to be setup in the env where the application master will be run
            LOG.info("Set the environment for the application master");
            Map<String, String> env = addEnvVariables();

            // Set the necessary command to execute the application master
            Vector<CharSequence> vargs = new Vector<CharSequence>(30);

            String appMasterMainClass = "org.nithinm.yarn.AppMaster";

            String appMasterParams = "metastore 2000 2050";
            // Set java executable command
            LOG.info("Setting up app master command");
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");

            // Set jar path
            // vargs.add(amJarUri);

            // Set class name
            vargs.add(appMasterMainClass);
            vargs.add(appMasterParams);

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            LOG.info("Completed setting up app master command " + command.toString());
            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());

            // Set up the container launch context for the application master
            ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, null, null);

            // Set up resource type requirements
            // For now, both memory and vcores are supported, so we set memory and
            // vcores requirements
            Resource capability = Resource.newInstance(amMemory, amVCores);
            appContext.setResource(capability);

            appContext.setAMContainerSpec(amContainer);

            // Set the priority for the application master
            // Priority pri = Priority.newInstance(amPriority);
            // appContext.setPriority(pri);

            // Set the queue to which this application is to be submitted in the RM
            // appContext.setQueue(amQueue);

            // Submit the application to the applications manager
            // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);

            yarnClient.submitApplication(appContext);

            int count = 0;
            while (count < 150) {
                // Get application report for the appId we are interested in
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                LOG.info("Current state: " + report.getYarnApplicationState().toString());
                if (report.getYarnApplicationState() == YarnApplicationState.FINISHED ||
                        report.getYarnApplicationState() == YarnApplicationState.FAILED ||
                        report.getYarnApplicationState() == YarnApplicationState.KILLED ||
                        report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
                    return report;
                }

                ThreadUtil.sleepAtLeastIgnoreInterrupts(5000);

                count++;
            }
        }
        catch (Exception e) {
            LOG.info("Caught exception:" + e.toString());
            e.printStackTrace();
        }

        return null;
    }

    public void CreateConfig(String defaultFS, String metastoreUrl)
    {
        /*Map<String, String> configurations = new HashMap<String, String>();

        // static ones
        //
        configurations.put("hive.execution.engine", "tez");
        configurations.put("dfs.blocksize", "268435456");
        configurations.put("hive.server2.enable.doAs", "false");
        // configurations.put("hive.exec.pre.hooks", "org.apache.hadoop.hive.ql.util.AdlHook");

        // system variables
        // TODO: this needs to changed dynamically
        //
        configurations.put("hive.metastore.uris", metastoreUrl);

        // user specific ones
        //
        configurations.put("fs.defaultFS", defaultFS);
        configurations.put("hive.exec.scratchdir", defaultFS + "hive/scratch");
        configurations.put("yarn.app.mapreduce.am.staging-dir", defaultFS + "hive/staging");
        configurations.put("hive.user.install.directory", defaultFS + "hive/user");
        configurations.put("hive.metastore.warehouse.dir", defaultFS + "hive/warehouse");
        // TODO: Following needs to be changed on every request by loading data from the request
        //
        configurations.put("javax.jdo.option.ConnectionPassword", "Sj58sVUMy6FUAo96Epp4XOfn3MLsLPM5AwLp26vvFLaWQkLXq0DZMlQAbckeQIb3fFKyAJPGeCdjYUroa81JifvCTTXaCeRqYbJ0bRNn8tXAWM");
        configurations.put("javax.jdo.option.ConnectionURL",
                "jdbc:sqlserver://ssudu92avt.database.windows.net:1433;database=v0303b91de3956bd64243ae6c6db5eefa9180hivemetastore;encrypt=true;trustServerCertificate=true;create=false;loginTimeout=300");
        configurations.put("javax.jdo.option.ConnectionUserName", "v0303b91de3956bd64243ae6c6db5eefa9180hivemetastoreLogin@ssudu92avt");

        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            // root elements
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("configuration");
            doc.appendChild(rootElement);

            Attr attr = doc.createAttribute("supports_final");
            attr.setValue("true");
            
            rootElement.setAttributeNode(attr);

            for (:
                 ) {
                
            }


        } catch (Exception e) {
            
        }

        
        StringBuilder builder = new StringBuilder();
        XmlWriter settings = new XmlWriter()
        {
            Indent = true,
            IndentChars = "\t",
            NewLineOnAttributes = true
        };
        settings.Encoding = Encoding.UTF8;

        using (XmlWriter writer = XmlWriter.Create(builder, settings))
        {
            // Write Processing Instruction
            String pi = "type=\"text/xsl\" href=\"configuration.xsl\"";
            writer.WriteProcessingInstruction("xml-stylesheet", pi);
            writer.WriteStartElement("configuration");
            writer.WriteAttributeString("supports_final", "true");

            foreach (KeyValuePair<string, string> entry in configurations)
            {
                writer.WriteStartElement("property");
                writer.WriteElementString("name", entry.Key);
                writer.WriteElementString("value", entry.Value);
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
        }

        return builder.ToString();*/
    }

    public ApplicationReport startHS2(String metastoreUrl) {
        try {
            YarnClient yarnClient = YarnClient.createYarnClient();
            conf = new Configuration();
            LOG.info("Conf:" + conf.toString());
            yarnClient.init(conf);
            yarnClient.start();

            YarnClientApplication app = yarnClient.createApplication();
            GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

            // set the application submission context
            ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
            ApplicationId appId = appContext.getApplicationId();

            appContext.setKeepContainersAcrossApplicationAttempts(false);
            appContext.setApplicationName(appName);

            // set local resources for the application master
            // local files or archives as needed
            // In this scenario, the jar file for the application master is part of the local resources
            Map<String, LocalResource> localResources = addLocalResources();
            FileSystem fs = FileSystem.get(conf);

            String execScript = storageAccount + "jars/hive-site.xml";
            Path scriptPath = fs.makeQualified(new Path(execScript)); // <- known path to jar file
            FileStatus scriptStatus = fs.getFileStatus(scriptPath);
            LocalResource scriptRsrc = Records.newRecord(LocalResource.class);
            // Set the type of resource - file or archive
            // archives are untarred at the destination by the framework
            scriptRsrc.setType(LocalResourceType.FILE);
            // Set visibility of the resource
            // Setting to most private option i.e. this file will only
            // be visible to this instance of the running application
            scriptRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            // Set the location of resource to be copied over into the
            // working directory
            scriptRsrc.setResource(ConverterUtils.getYarnUrlFromPath(scriptPath));
            // Set timestamp and length of file so that the framework
            // can do basic sanity checks for the local resource
            // after it has been copied over to ensure it is the same
            // resource the client intended to use with the application
            scriptRsrc.setTimestamp(scriptStatus.getModificationTime());
            scriptRsrc.setSize(scriptStatus.getLen());
            // The framework will create a symlink called AppMaster.jar in the
            // working directory that will be linked back to the actual file.
            // The ApplicationMaster, if needs to reference the jar file, would
            // need to use the symlink filename.
            localResources.put("hive-site.xml", scriptRsrc);

            String hiveScript = storageAccount + "jars/hivesamplescript.q";
            Path hivescriptPath = fs.makeQualified(new Path(hiveScript)); // <- known path to jar file
            FileStatus hiveScriptStatus = fs.getFileStatus(hivescriptPath);
            LocalResource hiveScriptRsrc = Records.newRecord(LocalResource.class);
            // Set the type of resource - file or archive
            // archives are untarred at the destination by the framework
            hiveScriptRsrc.setType(LocalResourceType.FILE);
            // Set visibility of the resource
            // Setting to most private option i.e. this file will only
            // be visible to this instance of the running application
            hiveScriptRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            // Set the location of resource to be copied over into the
            // working directory
            hiveScriptRsrc.setResource(ConverterUtils.getYarnUrlFromPath(hivescriptPath));
            // Set timestamp and length of file so that the framework
            // can do basic sanity checks for the local resource
            // after it has been copied over to ensure it is the same
            // resource the client intended to use with the application
            hiveScriptRsrc.setTimestamp(hiveScriptStatus.getModificationTime());
            hiveScriptRsrc.setSize(hiveScriptStatus.getLen());
            // The framework will create a symlink called AppMaster.jar in the
            // working directory that will be linked back to the actual file.
            // The ApplicationMaster, if needs to reference the jar file, would
            // need to use the symlink filename.
            localResources.put("script.q", hiveScriptRsrc);

            // Set the env variables to be setup in the env where the application master will be run
            LOG.info("Set the environment for the application master");
            Map<String, String> env = addEnvVariables();

            // Set the necessary command to execute the application master
            Vector<CharSequence> vargs = new Vector<CharSequence>(30);

            String appMasterMainClass = "org.nithinm.yarn.AppMaster";

            String appMasterParams = "metastore 2000 2050";
            // Set java executable command
            LOG.info("Setting up app master command");
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");

            // Set jar path
            // vargs.add(amJarUri);

            // Set class name
            vargs.add(appMasterMainClass);
            vargs.add(appMasterParams);

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            LOG.info("Completed setting up app master command " + command.toString());
            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());

            // Set up the container launch context for the application master
            ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, null, null);

            // Set up resource type requirements
            // For now, both memory and vcores are supported, so we set memory and
            // vcores requirements
            Resource capability = Resource.newInstance(amMemory, amVCores);
            appContext.setResource(capability);

            appContext.setAMContainerSpec(amContainer);

            // Set the priority for the application master
            // Priority pri = Priority.newInstance(amPriority);
            // appContext.setPriority(pri);

            // Set the queue to which this application is to be submitted in the RM
            // appContext.setQueue(amQueue);

            // Submit the application to the applications manager
            // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);

            yarnClient.submitApplication(appContext);

            int count = 0;
            while (count < 150) {
                // Get application report for the appId we are interested in
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                LOG.info("Current state: " + report.getYarnApplicationState().toString());
                if (report.getYarnApplicationState() == YarnApplicationState.FINISHED ||
                        report.getYarnApplicationState() == YarnApplicationState.FAILED ||
                        report.getYarnApplicationState() == YarnApplicationState.KILLED ||
                        report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
                    return report;
                }

                ThreadUtil.sleepAtLeastIgnoreInterrupts(5000);

                count++;
            }
        }
        catch (Exception e) {
            LOG.info("Caught exception:" + e.toString());
            e.printStackTrace();
        }

        return null;
    }
}
