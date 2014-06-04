package com.hazelcast.session;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.Role;
import org.apache.catalina.User;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.realm.UserDatabaseRealm;
import org.apache.catalina.startup.Embedded;
import org.apache.catalina.users.MemoryUserDatabase;
import org.apache.naming.NamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import javax.naming.NamingException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(HazelcastSerialClassRunner.class)
public class Tomcat6Test extends AbstractSessionReplicationTest {


    private static final String DEFAULT_HOST ="localhost" ;

    private static int TOMCAT_PORT_1=8899;
    private static int TOMCAT_PORT_2=8999;

    private Embedded tomcat1;
    private Embedded tomcat2;

    private Map<Integer,Embedded> webApps = new ConcurrentHashMap<Integer,Embedded>();

    @Before
    public void setup() throws Exception{
        tomcat1 = createCatalina(TOMCAT_PORT_1);
        tomcat2 = createCatalina(TOMCAT_PORT_2);
        tomcat1.start();
        tomcat2.start();


    }

    @After
    public void tearDown() throws Exception{
        tomcat1.stop();
        tomcat2.stop();
    }

    private Embedded createCatalina(int port) throws MalformedURLException {
        final Embedded catalina = new Embedded();

        final StandardServer server = new StandardServer();
        server.addService( catalina );

        try {
            final NamingContext globalNamingContext = new NamingContext( new Hashtable<String, Object>(), "ctxt" );
            server.setGlobalNamingContext( globalNamingContext );
            globalNamingContext.bind( "UserDatabase", createUserDatabase() );
        } catch ( final NamingException e ) {
            throw new RuntimeException( e );
        }


        final URL root = new URL( Tomcat6Test.class.getResource( "/" ), "../test-classes" );
        // use file to get correct separator char, replace %20 introduced by URL for spaces
        final String cleanedRoot = new File( root.getFile().replaceAll("%20", " ") ).toString();

        final String fileSeparator = File.separator.equals( "\\" ) ? "\\\\" : File.separator;
        final String docBase = cleanedRoot + File.separator + Tomcat6Test.class.getPackage().getName().replaceAll( "\\.", fileSeparator );

        final Engine engine = catalina.createEngine();
        engine.setName( "engine-" + port );
        engine.setDefaultHost( DEFAULT_HOST );
        engine.setJvmRoute( "tomcat-"+port );

        catalina.addEngine( engine );
        engine.setService( catalina );

        final UserDatabaseRealm realm = new UserDatabaseRealm();
        realm.setResourceName( "UserDatabase" );
        engine.setRealm( realm );

        final Host host = catalina.createHost( DEFAULT_HOST, docBase );
        engine.addChild( host );

        final Context context = createContext( catalina, "/", "webapp" );
        host.addChild( context );

        context.setManager( new HazelcastSessionManager());
        context.setBackgroundProcessorDelay( 1 );
        context.setCookies(true);
       // new File( "webapp" + File.separator + "webapp" ).mkdirs();

        final Connector connector = catalina.createConnector( "localhost", port, false );
        connector.setProperty("bindOnInit", "false");
        catalina.addConnector( connector );
        webApps.put(port,catalina);
        return catalina;
    }

    private Object createUserDatabase() {
        MemoryUserDatabase mud = new MemoryUserDatabase();
        final Role role = mud.createRole( "hzRole", "hz role" );
        final User user = mud.createUser( "hzUser", "hzPass", "hz user" );
        user.addRole(role);
        return mud;
    }

    protected Context createContext( final Embedded catalina, final String contextPath, final String docBase ) {
        return catalina.createContext( contextPath, docBase );
    }

    @Override
    public void reload(int port) {
        Embedded tomcat = webApps.get(port);
        Context ctx = (Context)tomcat.getContainer().findChild(DEFAULT_HOST).findChild("/");
        ctx.reload();
    }


}
