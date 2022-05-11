package common.meta;

import config.config;
import lombok.Data;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AXEwiges
 * @version 5.8
 * */
@Data
class logLoad implements Serializable {
    public List<String> Log;
    public String tableName;
    public Integer checkPoint;

    public logLoad(List<String> strings, String name, Integer integer) {
        Log = strings;
        tableName = name;
        checkPoint = integer;
    }
}

public class DMSLog {
    /**
    * 主要Log，存储全部的语句日志，key为表名，value为日志列表
    * */
    public ConcurrentHashMap<String, List<String>> mainLog;
    /**
     * 缓存Log，同步中的接收者的临时语句日志，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> bufferLog;
    /**
     * 临时Log，用于前述结构销毁和重建中的交换变量，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> tempLog;
    /**
     * 各表的传输时刻检查点，用于同步检验
     * */
    public ConcurrentHashMap<String, Integer> checkPoints;
    /**
     * 同步线程，不断监听，获取Log
     * */
    public syncRecvThread monitorThread;
    /**
     * 必要的config数据
     * */
    public config _LC;
    /**
     * 加入日志打印
     * */
    private static final Logger logger = Logger.getLogger(DMSLog.class);
    public DMSLog(config _C){
        _LC = _C;

        mainLog = new ConcurrentHashMap<>();
        bufferLog = new ConcurrentHashMap<>();
        tempLog = new ConcurrentHashMap<>();
        checkPoints = new ConcurrentHashMap<>();
        monitorThread = new syncRecvThread();

        monitorThread.start();
    }
    /**
     * 用于向日志中某个表项添加一条
     * @param tableName 表名
     * @param statement 语句
     * */
    public void add(String tableName, String statement){
        if(!mainLog.containsKey(tableName)){
            mainLog.put(tableName, new ArrayList<>());
            checkPoints.put(tableName, 0);
        }
        mainLog.get(tableName).add(statement);
        Integer integer = checkPoints.get(tableName) + 1;
        checkPoints.put(tableName, integer);
    }
    /**
     * @author AXEwiges
     *
     * 主线程，用于建立socket发送Log用于同步
     * */
    static class syncSendThread extends Thread {
        public static logLoad payload;
        private final Socket socket;
        public syncSendThread(logLoad payload, Socket socket) {
            syncSendThread.payload = payload;
            this.socket = socket;
        }

        @Override
        public void run() {
            logger.info("[Start SyncData] " + socket.toString() + " Table: " + payload.tableName);
            try{
                ObjectOutputStream sendOut = new ObjectOutputStream(socket.getOutputStream());
                sendOut.writeObject(payload);
                sendOut.flush();
                logger.info("[End SyncData] " + payload.tableName);
            } catch (Exception ignored) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    /**
     * @author AXEwiges
     * 主接受线程，用于监听端口并开启子线程处理新加入的同步Log
     * */
    class syncRecvThread extends Thread {
        public ServerSocket server;
        public syncRecvThread() {
            try {
                this.server = new ServerSocket(_LC.network.socketPort);
                logger.info("[Start Thread SyncRecv]");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            logger.info("[Start Recv Process, Listening] " + server.toString());
            while(true){
                Socket recv = null;
                try{
                    recv = server.accept();
                    logger.info("[SyncDB command from] " + recv.getRemoteSocketAddress());
                    Thread t = new syncDB(recv);
                    t.start();
                } catch (Exception ignored) {
                    try {
                        if(recv != null)
                            recv.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
    /**
     * @author AXEwiges
     * 子线程，用于监听端口并将接收到的数据写入mainLog
     * */
    class syncDB extends Thread {
        public Socket socket;
        public syncDB(Socket s){
            socket = s;
        }
        @Override
        public void run() {
            logger.info("[Start syncDB]");
            try{
                ObjectInputStream recvInputStream = new ObjectInputStream(socket.getInputStream());
                try{
                    logLoad log = (logLoad)recvInputStream.readObject();
                    for(String statement : log.Log)
                        add(log.tableName, statement);
                    synchronized(this){
                        logger.info("[Running syncLog]");
                        // TODO: Run command, wait DB debug finish.
                        logger.info("[Complete syncLog]");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                logger.info("[Complete syncDB]");
            } catch(Exception e){
                e.printStackTrace();
            }

        }
    }

    /**
     * 用于向目标服务器传输一个表的日志进行同步
     * @param ip ip地址
     * @param port 端口
     * @param tableName 表名
     * */
    public synchronized void transfer(String ip, String port, String tableName) {
        logLoad payload = new logLoad(mainLog.get(tableName), tableName, checkPoints.get(tableName));
        logger.info("[Payload ready, Wait for SyncData]");
        try {
            syncSendThread sender = new syncSendThread(payload, new Socket(ip, Integer.parseInt(port)));
            sender.start();
        } catch (Exception ignored) {

        }
    }
    /**
     * 用于接收日志，成功将通知master
     * */
    public static synchronized void receive(){
        // TODO: Callback to Master

    }

    public void testOutput(){
        for(Map.Entry<String, List<String>> m : mainLog.entrySet()){
            for(String s : m.getValue()){
                System.out.println(s);
            }
        }
    }
}
