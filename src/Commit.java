import java.io.*;
import java.util.*;

public class Commit implements Serializable {
    static final long serialVersionUID = -2895141894645277403L;

    public int commitId;
    public Map<String, byte[]> changedFiles;
    public Set<String> removedFiles;
    public long nanoTimestamp;
    public String author;
    public String message;

    public Commit() {
        commitId = 0;
        changedFiles = new HashMap<String, byte[]>();
        removedFiles = new HashSet<String>();
        nanoTimestamp = 0;
        author = "";
        message = "";
    }

    public boolean equals(Object o) {
        if (!(o instanceof Commit))
            return false;
        Commit x = (Commit) o;
        return commitId == x.commitId
            && changedFiles.equals(x.changedFiles)
            && removedFiles.equals(x.removedFiles)
            && nanoTimestamp == x.nanoTimestamp
            && author.equals(x.author)
            && message.equals(x.message);
    }
}
