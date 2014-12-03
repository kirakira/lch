import java.io.*;

public class Message implements Serializable {
    static final long serialVersionUID = -6406061878963820303L;
    public String replyAddress;
    public int replyPort;
    public String title;
    public Serializable content;

    public Message(String replyAddress, int replyPort, String title,
            Serializable content) {
        this.replyAddress = replyAddress;
        this.replyPort = replyPort;
        this.title = title;
        this.content = content;
    }
}
