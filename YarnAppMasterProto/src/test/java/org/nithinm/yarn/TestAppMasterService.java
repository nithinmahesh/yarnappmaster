package org.nithinm.yarn;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * Created by nithinm on 4/6/2016.
 */
public class TestAppMasterService {
    AppMasterService appMasterService;

    @Before
    public void setUp() throws Exception {
        appMasterService = new AppMasterService(AppMasterService.Service.METASTORE.toString(), 1, 16000);
        boolean result = appMasterService.start();
        assertEquals("Failed to start application master", true, result);
    }

    @After
    public void tearDown() throws Exception {
        boolean result = appMasterService.shutdown();
        assertEquals("Failed to stop application master", true, result);
    }

    @Test
    public void checkMetastoreAccess() throws Exception {
        String url = appMasterService.getMetastoreUrl();
        assertNotEquals("Null metastore URL", "", url);
        assertTrue("Invalid URL", url.endsWith(":"));

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        String charset = "UTF-8";
        connection.setRequestProperty("Accept-Charset", charset);
        InputStream response = connection.getInputStream();
        int status = connection.getResponseCode();
        assertNotEquals("Metastore did not come up successfully", HttpURLConnection.HTTP_NOT_FOUND, status);
    }

}