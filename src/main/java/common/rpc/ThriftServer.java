package common.rpc;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThriftServer {

    private final TServer server;
    private final int port;

    public ThriftServer(TProcessor processor, int port) throws TTransportException {
        this.port = port;
        TServerSocket serverTransport = new TServerSocket(9090);
        server = new TThreadPoolServer(
                new TThreadPoolServer
                        .Args(serverTransport)
                        .processor(processor)
                        .protocolFactory(new TBinaryProtocol.Factory()));
    }

    public int getPort() {
        return port;
    }

    public void startServer() {
        server.serve();
    }

}
