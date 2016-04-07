package org.nithinm.yarn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Created by nithinm on 3/29/2016.
 * Class for managing a http server
 */
public class WebApp {
    private static final Log LOG = LogFactory.getLog(AppMasterService.class);
    private int webServerPort;
    private String httpPath;
    private HttpServer server;
    private AppMasterService appMasterService;

    public WebApp(int minport, int maxport, AppMasterService amService) {
        int availablePort = PortAvailabilityDetector.getAvailablePortInRange(minport, maxport);
        if (availablePort != 0) {
            webServerPort = availablePort;
        } else {
            webServerPort = 0;
        }

        server = null;
        httpPath = "/CustomAppMaster";
        appMasterService = amService;
    }

    public boolean startServer() {
        try {
            if (webServerPort == 0) {
                throw new Exception("No ports available to start http server.");
            }

            if(server == null) {
                server = HttpServer.create(new InetSocketAddress(webServerPort), 0);
                server.createContext(httpPath, new AMStatusReportHandler());
                server.setExecutor(null);
                server.start();
                return true;
            }
        }
        catch(Exception e) {
            LOG.error("error starting jetty server: " + e.getMessage());
        }

        return false;
    }

    public boolean stopServer() {
        try {
            if(server != null) {
                server.stop(0);
                server = null;
            }

            return true;
        }
        catch(Exception e) {
            LOG.error("error stopping jetty server: " + e.getMessage());
        }

        return false;
    }

    public String getServerUrl() {
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("error resolving hostname: " + e.getMessage());
        }
        return "http://" + hostname + ":" + webServerPort + httpPath;
    }

    class AMStatusReportHandler implements HttpHandler {

        public void handle(HttpExchange he) throws IOException {
            if (he.getRequestMethod().equalsIgnoreCase("GET")) {
                String resp = "{\"URL\":\"" + appMasterService.getMetastoreUrl() + "\"}";
                he.sendResponseHeaders(200, resp.length());
                OutputStream os = he.getResponseBody();
                os.write(resp.getBytes());
                os.close();
            } else if (he.getRequestMethod().equalsIgnoreCase("POST")) {
                String resp = "Processing close request...";
                he.sendResponseHeaders(200, resp.length());
                OutputStream os = he.getResponseBody();
                os.write(resp.getBytes());
                os.close();

                appMasterService.shutdown();
            }
        }
    }

}
