import java.io.*;

public class CommitRequest implements Serializable {
    static final long serialVersionUID = -3090374083026363629L;

    public String responseTitle;
    public int baseCommit;
    public Commit proposedCommit;
}
