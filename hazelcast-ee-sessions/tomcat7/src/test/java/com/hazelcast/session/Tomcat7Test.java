package com.hazelcast.session;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.junit.runner.RunWith;

import javax.servlet.ServletException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(HazelcastSerialClassRunner.class)
public abstract class Tomcat7Test extends AbstractSessionReplicationTest {

    protected Tomcat tomcat1;
    protected Tomcat tomcat2;

    protected static int TOMCAT_PORT_1=8899;
    protected static int TOMCAT_PORT_2=8999;

    private Map<Integer,Context> webApps = new ConcurrentHashMap<Integer,Context>();

    protected Tomcat createServer(int port,HazelcastSessionManager manager) throws MalformedURLException, ServletException {

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



        context.setManager(manager);
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
