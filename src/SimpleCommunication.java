import java.io.*;

public class SimpleCommunication {
    public static final int portA = 12345, portB = 23456;

    public static final void main(String[] args) {
        NetIO netA = new NetIO(portA);
        NetIO netB = new NetIO(portB);
        System.out.println("Server created");
        Thread ta = new Thread(new A(netA));
        Thread tb = new Thread(new B(netB));
        ta.start();
        tb.start();
        try {
            ta.join();
            tb.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Finished");
        netA.close();
        netB.close();
    }

    public static class MyMessage implements Serializable {
        static final long serialVersionUID = -9220412206804979135L;
        public int value;
        public String string;

        public MyMessage(int v, String s) {
            this.value = v;
            this.string = s;
        }
    }

    public static class A implements Runnable {
        public volatile NetIO net;

        public A(NetIO net) {
            this.net = net;
        }

        public void run() {
            net.sendMessage("localhost", portB, "Greeting", new MyMessage(13, "Ha Ha ha.."));
            display("A", net.receiveMessage("Bye", 10 * NetIO.numNanosPerSecond));
            net.close();
        }
    }

    public static class B implements Runnable {
        public volatile NetIO net;

        public B(NetIO net) {
            this.net = net;
        }

        public void run() {
            display("B", net.receiveMessage("Greeting", 10 * NetIO.numNanosPerSecond));
            net.sendMessage("localhost", portA, "irrelavent", new MyMessage(13, "This message should not be displayed"));
            net.sendMessage("localhost", portA, "Bye", new MyMessage(0, "Gun cu!"));
            net.close();
        }
    }

    public static void display(String who, Message m) {
        MyMessage mm = (MyMessage) m.content;
        System.out.println(who + " received a message from "
                + m.senderAddress + ":" + m.senderPort
                + " titled " + m.title + ": " + mm.value + ", " + mm.string);
    }
}
