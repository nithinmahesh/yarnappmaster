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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by nithinm on 2/18/2016.
 */
public class MyYarnClient extends Configured {
    private String appName = "MyTestApp";
    private static final Log LOG = LogFactory.getLog(MyYarnClient.class);
    private Configuration conf;
    private int amMemory = 8192;
    private int amVCores = 4;

    public void startYarnApp() {
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
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            LOG.info("Copy App Master jar from local filesystem and add to local environment");

            FileSystem fs = FileSystem.get(conf);

            String amJarUri = "wasb://nithinhivetest@nithinhivetest.blob.core.windows.net/jars/YarnAppMasterProto-1.0-SNAPSHOT.jar";
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

            String HS2JarUri = "wasb://nithinhivetest@nithinhivetest.blob.core.windows.net/jars/hive-0.14.0.2.2.7.1-34.tar";
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

            // Set the env variables to be setup in the env where the application master will be run
            LOG.info("Set the environment for the application master");
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

            // Set the necessary command to execute the application master
            Vector<CharSequence> vargs = new Vector<CharSequence>(30);

            String appMasterMainClass = "org.nithinm.yarn.AppMaster";
            // Set java executable command
            LOG.info("Setting up app master command");
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");

            // Set jar path
            // vargs.add(amJarUri);

            // Set class name
            vargs.add(appMasterMainClass);

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

            // Setup security tokens
            if (UserGroupInformation.isSecurityEnabled()) {
                // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
                Credentials credentials = new Credentials();
                String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
                if (tokenRenewer == null || tokenRenewer.length() == 0) {
                    throw new IOException(
                            "Can't get Master Kerberos principal for the RM to use as renewer");
                }
/*
            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);*/
            }

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
                if (report.getYarnApplicationState() == YarnApplicationState.FINISHED){
                    break;
                }

                ThreadUtil.sleepAtLeastIgnoreInterrupts(5000);

                count++;
            }
        }
        catch (Exception e) {
            LOG.info("Caught exception:" + e.toString());
            e.printStackTrace();
        }
    }
}
