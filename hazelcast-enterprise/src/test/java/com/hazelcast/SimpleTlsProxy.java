package com.hazelcast;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.close;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadKeyManagerFactory;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadTrustManagerFactory;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class SimpleTlsProxy {

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final InetSocketAddress proxyAddress;
    private final InetSocketAddress target;

    private final ServerSocket serverSocket;
    private final SSLSocketFactory sslSocketFactory;
    private final ExecutorService executor;
    private volatile Thread acceptor;

    public SimpleTlsProxy(int port, String keyStorePath, String keyStorePwd,
                          String trustStorePath, String trustStorePwd, InetSocketAddress target, int numOfConnections) {

        this.target = target;
        try {
            this.proxyAddress = new InetSocketAddress(InetAddress.getLocalHost(), port);
        } catch (UnknownHostException e) {
            throw rethrow(e);
        }

        SSLContext sslContext = buildSslContext(keyStorePath, keyStorePwd, trustStorePath, trustStorePwd);

        executor = Executors.newFixedThreadPool(numOfConnections);
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(proxyAddress);

            sslSocketFactory = sslContext.getSocketFactory();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private SSLContext buildSslContext(String keyStorePath, String keyStorePwd,
                                       String trustStorePath, String trustStorePwd) {
        KeyManagerFactory kmf;
        TrustManagerFactory tmf;
        try {
            kmf = loadKeyManagerFactory(keyStorePwd, keyStorePath, KeyManagerFactory.getDefaultAlgorithm());
            tmf = loadTrustManagerFactory(trustStorePwd, trustStorePath, TrustManagerFactory.getDefaultAlgorithm());
        } catch (Exception e) {
            throw rethrow(e);
        }

        final SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("TLS");
        } catch (NoSuchAlgorithmException e) {
            throw rethrow(e);
        }
        try {
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (KeyManagementException e) {
            throw rethrow(e);
        }

        return sslContext;
    }

    public InetSocketAddress getProxyAddress() {
        return proxyAddress;
    }

    public void start() {
        running.set(true);
        acceptor = new Thread() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        Socket clientS = serverSocket.accept();
                        clientS.setSoLinger(true, 0);
                        clientS.setTcpNoDelay(true);

                        Socket targetS = sslSocketFactory.createSocket(target.getAddress(), target.getPort());
                        targetS.setSoLinger(true, 0);
                        targetS.setTcpNoDelay(true);
                        executor.execute(new Handler(clientS, targetS));
                    } catch (IOException e) {
                        if (running.get()) {
                            throw rethrow(e);
                        }
                    }
                }
            }
        };
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public void stop()
            throws InterruptedException {
        running.set(false);
        close(serverSocket);
        acceptor.interrupt();
        acceptor.join();
        executor.shutdownNow();
        executor.awaitTermination(15, TimeUnit.SECONDS);
    }

    class Handler
            implements Runnable {

        private final Socket client;
        private final Socket target;

        private final ExecutorService executor = Executors.newFixedThreadPool(2);

        Handler(Socket client, Socket target) {
            this.client = client;
            this.target = target;
        }

        public void run() {

            try {
                InputStream clientIn = client.getInputStream();
                OutputStream clientOut = client.getOutputStream();

                InputStream targetIn = target.getInputStream();
                OutputStream targetOut = target.getOutputStream();

                Future outForwardPipe = executor.submit(new Pipe(client + "->" + target, clientIn, targetOut));
                Future inForwardPipe = executor.submit(new Pipe(target + "->" + client, targetIn, clientOut));

                outForwardPipe.get();
                inForwardPipe.get();
            } catch (InterruptedException i) {
                ignore(i);
            } catch (Exception ioe) {
                throw rethrow(ioe);
            } finally {
                close(client);
                close(target);
                executor.shutdownNow();
                try {
                    executor.awaitTermination(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }
        }

        private class Pipe implements Runnable {

            private final InputStream in;
            private final OutputStream out;
            private final String pipeDirection;

            Pipe(String pipeDirection, InputStream is, OutputStream os) {
                this.in = is;
                this.out = os;
                this.pipeDirection = pipeDirection;
            }

            @Override
            public void run() {
                try {
                    int n;
                    byte[] buffer = new byte[1024];
                    while ((n = in.read(buffer)) > -1) {
                        out.write(buffer, 0, n);
                    }
                } catch (IOException e) {
                    throw rethrow(new IllegalStateException(pipeDirection, e));
                } finally {
                    closeResource(in);
                    closeResource(out);
                }
            }
        }
    }

}
