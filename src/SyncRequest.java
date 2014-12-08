import java.io.*;

public class SyncRequest implements Serializable {
    static final long serialVersionUID = 751214529803565664L;

    public String responseTitle;
    public int baseCommit;

    public String toString() {
        return "base commit: " + baseCommit;
    }
}
