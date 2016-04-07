package org.nithinm.yarn;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import static org.junit.Assert.*;

/**
 * Created by nithinm on 4/6/2016.
 */
public class TestWebApp {
    WebApp webApp;
    AppMasterService appMasterService;
    String serverUrl;

    @Before
    public void setUp() throws Exception {
        appMasterService = new AppMasterService(AppMasterService.Service.BEELINE.toString(),  1, 16000);
        webApp = new WebApp(1, 16000, appMasterService);
        boolean result = webApp.startServer();
        assertEquals("Failed to start http server", true, result);
    }

    @After
    public void tearDown() throws Exception {
        boolean result = webApp.stopServer();
        assertEquals("Failed to stop http server", true, result);
    }

    @Test
    public void getMetastoreUrl() throws Exception {
        serverUrl = webApp.getServerUrl();
        assertTrue("Hostname not resolved correctly", !serverUrl.contains("localhost"));
        HttpURLConnection connection = (HttpURLConnection) new URL(serverUrl).openConnection();
        String charset = "UTF-8";
        connection.setRequestProperty("Accept-Charset", charset);
        InputStream response = connection.getInputStream();
        int status = connection.getResponseCode();
        assertEquals("HTTP response code returned:" + status, HttpURLConnection.HTTP_OK, status);
    }

}