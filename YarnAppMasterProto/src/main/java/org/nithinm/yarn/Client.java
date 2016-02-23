package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nithinm.yarn.MyYarnClient;

/**
 * Created by nithinm on 2/18/2016.
 */
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);
    public static void main(String[] args) {
        LOG.info("Starting client.");
        MyYarnClient yarnClient = new MyYarnClient();
        LOG.info("Starting yarn app.");
        yarnClient.startYarnApp();
        LOG.info("Done");
    }
}
