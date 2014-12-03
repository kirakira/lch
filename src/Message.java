import java.io.*;

public class Message implements Serializable {
    static final long serialVersionUID = -6406061878963820303L;
    public String senderAddress;
    public int senderPort;
    public String title;
    public Serializable content;

    public Message(String senderAddress, int senderPort, String title,
            Serializable content) {
        this.senderAddress = senderAddress;
        this.senderPort = senderPort;
        this.title = title;
        this.content = content;
    }
}
