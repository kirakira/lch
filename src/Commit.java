import java.io.*;
import java.util.*;

public class Commit implements Serializable {
    static final long serialVersionUID = -2895141894645277403L;

    public int commitId;
    public HashMap<String, byte[]> changedFiles;
    public Set<String> removedFiles;
    public long nanoTimestamp;
    public String author;
    public String message;
}
