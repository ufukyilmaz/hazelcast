package com.hazelcast.session;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by mesutcelik on 5/5/14.
 */
public abstract class AbstractMapNameTest extends HazelcastTestSupport {

    protected static int SERVER_PORT_1 = 8899;
    protected static int SERVER_PORT_2 = 8999;
    protected static String SESSION_REPLICATION_MAP_NAME = "session-replication-map";

    protected HazelcastInstance instance;

    @Test
    public void testMapName() throws Exception{
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", SERVER_PORT_1, cookieStore);

        Cookie cookie = cookieStore.getCookies().get(0);
        String sessionId = cookie.getValue();

        IMap<String,HazelcastSession> map = instance.getMap(SESSION_REPLICATION_MAP_NAME);
        assertEquals(1,map.size());
        HazelcastSession session = map.get(sessionId);

        assertFalse(session.getLocalAttributeCache().isEmpty());

        executeRequest("remove", SERVER_PORT_2, cookieStore);
        cookie = cookieStore.getCookies().get(0);
        String newSessionId = cookie.getValue();
        session = map.get(newSessionId);

        assertTrue(session.getLocalAttributeCache().isEmpty());

    }


    protected String executeRequest(String context, int serverPort, CookieStore cookieStore) throws Exception {
        HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();
        HttpGet request = new HttpGet("http://localhost:" + serverPort + "/" + context);
        HttpResponse response = client.execute(request);
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

}
