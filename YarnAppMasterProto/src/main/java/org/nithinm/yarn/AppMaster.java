package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by nithinm on 2/17/2016.
 * Entry point for custom Application Master
 */
public class AppMaster {
    private static final Log LOG = LogFactory.getLog(AppMaster.class);
    private static String usageMsg = "Usage:\n"
            + "org.nithinm.yarn.AppMaster <serviceName> <additionalServiceOptions>\n"
            + "Valid values for serviceName(case insensitive):\n"
            + "metastore\n"
            + "hiveserver2\n"
            + "Valid values for additionalServiceOptions:\n"
            + "When serviceName is metastore, <minport> <maxport> for metastore needs to be specified as the next params\n";

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 3) {
            LOG.info(usageMsg);
            throw new Exception(usageMsg);
        }

        // First args is service name
        //
        String serviceName = args[0];
        int minport = 0;
        int maxport = 0;

        if (serviceName.equalsIgnoreCase(AppMasterService.Service.METASTORE.toString())) {
            if (args.length != 3) {
                LOG.info(usageMsg);
                throw new Exception(usageMsg);
            }

            minport = Integer.parseInt(args[1]);
            maxport = Integer.parseInt(args[2]);
        }

        AppMasterService service = new AppMasterService(serviceName, minport, maxport);
        service.start();
        service.waitForCompletion();
    }
}
