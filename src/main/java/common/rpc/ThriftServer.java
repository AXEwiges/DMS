package common.rpc;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftServer {

    private TServer server;
    public int port;
    private TProcessor processor;

    public ThriftServer(TProcessor processor, int port) throws TTransportException {
        this.port = port;
        TServerTransport serverTransport = new TServerSocket(9090);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();

    }

    public void startServer() {

    }
}
