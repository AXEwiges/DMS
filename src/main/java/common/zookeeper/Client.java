package common.zookeeper;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import common.meta.ClientInfo;

/**
 * 管理 Zookeeper 连接。
 *
 * 本类的名称是针对 zookeeper 而言的，Master 和 Region Server 都属于
 * Zookeeper 的 client。
 */
public interface Client {

    /**
     * 连接到 Zookeeper 服务器。
     *
     * @param zkHostPort Zookeeper 服务器主机和端口，如 {@code 192.168.1.1:2181}
     * @param clientInfo 本 client 信息
     * @param sessionTimeout 连接超时（毫秒）
     * @return 当前连接的 Master 的 ClientInfo 对象（可能是自己）
     */
    ClientInfo connect(String zkHostPort, ClientInfo clientInfo, int sessionTimeout)
        throws IOException, InterruptedException, KeeperException;
}
