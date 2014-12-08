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
        if (!equalsChangedFiles(changedFiles, x.changedFiles))
            return false;
        return commitId == x.commitId
            && removedFiles.equals(x.removedFiles)
            && nanoTimestamp == x.nanoTimestamp
            && author.equals(x.author)
            && message.equals(x.message);
    }

    public static class ArrayWrapper {
        public byte[] a;

        public ArrayWrapper(byte[] x) {
            a = x;
        }

        public int hashCode() {
            return Arrays.hashCode(a);
        }

        public boolean equals(Object t) {
            if (!(t instanceof ArrayWrapper))
                return false;
            return Arrays.equals(a, ((ArrayWrapper) t).a);
        }
    }

    public static boolean equalsChangedFiles(Map<String, byte[]> a, Map<String, byte[]> b) {
        Map<String, ArrayWrapper> ma = new HashMap<String, ArrayWrapper>(),
            mb = new HashMap<String, ArrayWrapper>();
        for (Map.Entry<String, byte[]> t: a.entrySet())
            ma.put(t.getKey(), new ArrayWrapper(t.getValue()));
        for (Map.Entry<String, byte[]> t: b.entrySet())
            mb.put(t.getKey(), new ArrayWrapper(t.getValue()));
        return ma.equals(mb);
    }

    public String toString() {
        String ret = "commit id: " + commitId;
        ret += ", changed files: " + changedFiles.size();
        ret += ", removed files: " + removedFiles.size();
        ret += ", timestamp: " + nanoTimestamp;
        ret += ", author: " + author;
        ret += ", message: " + message;
        return ret;
    }
}
