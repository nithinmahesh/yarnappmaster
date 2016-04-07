package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Created by nithinm on 3/29/2016.
 * Helper class to identify available ports on a machine
 */
public class PortAvailabilityDetector {
    private static final Log LOG = LogFactory.getLog(PortAvailabilityDetector.class);

    public static int getAvailablePortInRange(int minport, int maxport) {
        assert(minport <= maxport);

        for (int i = minport; i <= maxport; i++) {
            if (isPortAvailable(i)) {
                LOG.info("Identified port <" + i +
                            "> as available between ports <" +
                            minport + "> and <"+ maxport + ">.");
                return i;
            }
        }

        return 0;
    }

    private static boolean isPortAvailable(int port) {
        boolean isAvailable = false;
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            isAvailable = true;
        } catch (IOException e) {
            /* should not be thrown */
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                /* should not be thrown
                * Need to be handled */
                    LOG.warn("ServerSocket close threw an exception:" + e.getMessage());
                }
            }
        }

        return isAvailable;
    }
}
