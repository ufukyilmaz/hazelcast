package com.hazelcast.session;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import javax.servlet.ServletException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(HazelcastSerialClassRunner.class)
public class Tomcat7Test extends AbstractSessionReplicationTest {



    private Tomcat tomcat1;
    private Tomcat tomcat2;

    private Map<Integer,Context> webApps = new ConcurrentHashMap<Integer,Context>();

    @Before
    public void setup() throws Exception{
        tomcat1 = createServer(SERVER_PORT_1);
        tomcat2 = createServer(SERVER_PORT_2);
        tomcat1.start();
        tomcat2.start();
    }

    @After
    public void tearDown() throws Exception{
        tomcat1.stop();
        tomcat2.stop();
    }

    private Tomcat createServer(int port) throws MalformedURLException, ServletException {

        final URL root = new URL( TestServlet.class.getResource( "/" ), "../../../sessions-core/target/test-classes" );
        // use file to get correct separator char, replace %20 introduced by URL for spaces
        final String cleanedRoot = new File( root.getFile().replaceAll("%20", " ") ).toString();

        final String fileSeparator = File.separator.equals( "\\" ) ? "\\\\" : File.separator;
        final String docBase = cleanedRoot + File.separator + TestServlet.class.getPackage().getName().replaceAll( "\\.", fileSeparator );

        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(docBase);

        tomcat.getEngine().setName("engine-"+port);

        final Connector connector = tomcat.getConnector();
        connector.setPort(port);
        connector.setProperty("bindOnInit", "false");

                Context context;
        try {
            context = tomcat.addWebapp(tomcat.getHost(),"/", docBase + fileSeparator + "webapp");
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }



        context.setManager(new HazelcastSessionManager());
        context.setCookies(true);
        context.setBackgroundProcessorDelay(1);
        context.setReloadable(true);

        webApps.put(port, context);

        return tomcat;

    }


    @Override
    public void reload(int port) {
        webApps.get(port).reload();
    }
}
