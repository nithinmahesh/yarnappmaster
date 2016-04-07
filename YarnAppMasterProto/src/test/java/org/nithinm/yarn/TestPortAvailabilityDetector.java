package org.nithinm.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by nithinm on 4/6/2016.
 */
public class TestPortAvailabilityDetector {
    private static final Log LOG = LogFactory.getLog(TestPortAvailabilityDetector.class);

    @Test
    public void testPortAvailability()
    {
        int startport  = 1, endport = 16000;

        Random rand = new Random();
        int outputPort = rand.nextInt(endport);

        ServerSocket[] ss = new ServerSocket[outputPort];
        for (int i = 0; i < outputPort; i++) {
            try {
                ss[i] = new ServerSocket(i);
                ss[i].setReuseAddress(true);
            }
            catch (Exception e) {}
        }

        int availablePort = PortAvailabilityDetector.getAvailablePortInRange(startport, endport);

        for (int i = 0; i < outputPort; i++) {
            try {
                if (ss[i] != null) {
                    ss[i].close();
                }
            } catch (IOException e) {
                /* should not be thrown
                * Need to be handled */
                LOG.warn("ServerSocket close threw an exception:" + e.getMessage());
            }
        }

        assertTrue("Bad port identified", availablePort >= outputPort);
    }

}
