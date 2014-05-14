package com.hazelcast.session;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by mesutcelik on 5/5/14.
 */
public abstract class AbstractSessionReplicationTest extends HazelcastTestSupport {

    protected static int SERVER_PORT_1 = 8899;
    protected static int SERVER_PORT_2 = 8999;


    @Test
    public void testContextReloadSticky() throws Exception{
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);
        reload(SERVER_PORT_1);

        String value = executeRequest("read", SERVER_PORT_1, cookieStore);
        assertEquals("value", value);
    }

    @Test
    public void testContextReloadNonSticky() throws Exception{
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);
        reload(SERVER_PORT_1);

        String value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("value", value);
    }

    @Test
    public void testReadWriteRead() throws Exception{
        CookieStore cookieStore = new BasicCookieStore();
        String value = executeRequest("read", SERVER_PORT_1, cookieStore);
        assertEquals("null", value);

        executeRequest("write", SERVER_PORT_2, cookieStore);

        value = executeRequest("read", SERVER_PORT_1, cookieStore);
        assertEquals("value", value);

    }

    @Test(timeout = 60000)
    public void testAttributeDistribution() throws Exception {

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);

        String value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("value", value);
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval() throws Exception {

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);


        String value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("remove", SERVER_PORT_2, cookieStore);
        assertEquals("true", value);

        value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("null", value);
    }

    @Test(timeout = 60000)
    public void testAttributeUpdate() throws Exception {

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);

        String value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("update", SERVER_PORT_2, cookieStore);
        assertEquals("true", value);

        value = executeRequest("read", SERVER_PORT_1, cookieStore);
        assertEquals("value-updated", value);
    }

    @Test(timeout = 60000)
    public void testAttributeInvalidate() throws Exception {

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);

        String value = executeRequest("read", SERVER_PORT_2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("invalidate", SERVER_PORT_2, cookieStore);
        assertEquals("true", value);

        HazelcastInstance instance = createHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("default");
        assertEquals(0, map.size());
    }

    public abstract void reload(int port);


    protected String executeRequest(String context, int serverPort, CookieStore cookieStore) throws Exception {
        HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();
        HttpGet request = new HttpGet("http://localhost:" + serverPort + "/" + context);
        HttpResponse response = client.execute(request);
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

}
