import java.util.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.net.*;

/**
 * This class is thread-safe.
 */
public class NetIO {
    public static final long numNanosPerSecond = 1000000000L;

    /**
     * Set up a server on the given port.
     * If anything wrong happened, a RuntimeException will be thrown.
     */
    public NetIO(int port) {
        try {
            serverSocket = new ServerSocket(port);
            new Thread(new ServerThread()).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Send a message to a given host, with the specified title and message body.
     */
    public void sendMessage(String host, int port, String title, Serializable message) {
        try {
            MessageSocket socket = new MessageSocket(
                    new Socket(host, port));
            socket.sendMessage(new Message(null, serverSocket.getLocalPort(),
                        title, message));
            socket.close();
        } catch (UnknownHostException e) {
           e.printStackTrace();
        } catch (IOException e) {
           e.printStackTrace();
        }
    }
    
    public void sendMessage(String hostAndPort, String title, Serializable message) {
        int pos = hostAndPort.indexOf(":");
        if (pos != -1) {
            String host = hostAndPort.substring(0, pos);
            int port = Integer.parseInt(hostAndPort.substring(pos + 1));
            sendMessage(host, port, title, message);
        }
    }

    /**
     * Wait for a message with the given title, or until time expires. If more than
     * one thread is waiting for the message with the same title, an arbitrary thread
     * will be selected to receive the message.
     *
     * If time expired before the message is received, returns null; otherwise returns
     * the message received.
     *
     * timeout is in nanoseconds.
     */
    public Message receiveMessage(String title, long timeout) {
        LockAndQueue lq;
        synchronized(receivedMessages) {
            if (!receivedMessages.containsKey(title))
                receivedMessages.put(title, new LockAndQueue());
            lq = receivedMessages.get(title);
        }
        lq.lock.lock();
        try {
            while (lq.queue.size() == 0) {
                timeout = lq.condition.awaitNanos(timeout);
                if (timeout <= 0L)
                    return null;
            }
            return lq.queue.poll();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } finally {
            lq.lock.unlock();
        }
    }

    /**
     * Close the server.
     */
    public void close() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ServerThread implements Runnable {
        public void run() {
            try {
                while (true) {
                    new Thread(new HandleClientThread(serverSocket.accept())).start();
                }
            } catch (SocketException e) {
                /* Socet is being closed */
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private class HandleClientThread implements Runnable {
        private volatile Socket socket;

        public HandleClientThread(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                MessageSocket messageSocket = new MessageSocket(socket);
                Message msg = (Message) messageSocket.receiveMessage();
                msg.replyAddress = socket.getInetAddress().getHostAddress();

                LockAndQueue lq;
                synchronized(receivedMessages) {
                    if (!receivedMessages.containsKey(msg.title))
                        receivedMessages.put(msg.title, new LockAndQueue());
                    lq = receivedMessages.get(msg.title);
                }

                lq.lock.lock();
                try {
                    lq.queue.add(msg);
                    lq.condition.signal();
                } finally {
                    lq.lock.unlock();
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println("Corrupted message received or IO exception");
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private volatile ServerSocket serverSocket;
    private final Map<String, LockAndQueue> receivedMessages
        = new HashMap<String, LockAndQueue>();
}

class LockAndQueue {
    final Lock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();
    final Queue<Message> queue = new LinkedList<Message>();
}

class MessageSocket {
    private Socket socket;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;

    public MessageSocket(Socket socket) throws IOException {
        this.socket = socket;
        oos = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        oos.flush();
        ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
    }

    public void sendMessage(Serializable message) throws IOException {
        oos.writeObject(message);
        oos.flush();
    }

    public Serializable receiveMessage() throws ClassNotFoundException, IOException {
        return (Serializable) ois.readObject();
    }

    public void close() {
        try {
            oos.close();
            ois.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
