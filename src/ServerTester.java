import java.util.*;

public class ServerTester {
    public static final void main(String[] args) {
        testPaxos();
    }

    public static void testPaxos() {
        int n = 10;
        int basePort = 12345;
        LchServer[] servers = new LchServer[n];
        NetIO[] clients = new NetIO[n];

        List<String> serverList = new ArrayList<String>();
        for (int i = 0; i < n; ++i)
            serverList.add("localhost:" + (basePort + i));
        for (int i = 0; i < n; ++i)
            servers[i] = new LchServer(basePort + i, i, serverList, false, null);
        for (int i = 0; i < n; ++i)
            clients[i] = new NetIO(basePort + i + n);

        for (int j = 0; j < 10; ++j) {
            System.out.println("Round " + j + 1);
            CommitRequest[] cr = new CommitRequest[n];
            for (int i = 0; i < n; ++i) {
                cr[i] = new CommitRequest();
                cr[i].baseCommit = getVersion(serverList.get(i), clients[i]);
                cr[i].responseTitle = randomTitle();
                cr[i].proposedCommit = new Commit();
                cr[i].proposedCommit.commitId = cr[i].baseCommit + 1;
                cr[i].proposedCommit.message = "" + i;
            }
            for (int i = 0; i < n; ++i)
                clients[i].sendMessage(serverList.get(i), "CommitRequest", cr[i]);
            int accepted = 0, rejected = 0;
            for (int i = 0; i < n; ++i) {
                CommitResponse msg = (CommitResponse)
                    clients[i].receiveMessage(cr[i].responseTitle, 10 * NetIO.numNanosPerSecond).content;
                if (msg.accepted) {
                    ++accepted;
                    System.out.println(i + " accepted");
                } else
                    ++rejected;
            }
            System.out.println("accepted: " + accepted + ", rejected: " + rejected);
            if (accepted != 1) {
                System.out.println("test failed!");
                break;
            }
        }

        int v = checkVersions(serverList, clients);
        if (v == -1)
            System.out.println("Versions are inconsistent!");
        else
            System.out.println("Versions are consistent. Current version: " + v);

        for (int i = 0; i < n; ++i)
            clients[i].close();
        for (int i = 0; i < n; ++i)
            servers[i].close();
    }

    public static int getVersion(String server, NetIO net) {
        SyncRequest syncRequest = new SyncRequest();
        syncRequest.baseCommit = 0;
        syncRequest.responseTitle = randomTitle();
        net.sendMessage(server, "SyncRequest", syncRequest);
        SyncResponse msg = (SyncResponse)
            net.receiveMessage(syncRequest.responseTitle, 10 * NetIO.numNanosPerSecond).content;
        if (msg.commits.size() == 0)
            return 0;
        return msg.commits.get(msg.commits.size() - 1).commitId;
    }

    public static final int checkVersions(List<String> servers, NetIO[] clients) {
        int[] v = new int[servers.size()];
        String out = "";
        for (int i = 0; i < servers.size(); ++i) {
            v[i] = getVersion(servers.get(i), clients[i]);
            out += v[i] + " ";
        }
        System.out.println(out);
        for (int i = 1; i < v.length; ++i)
            if (v[i] != v[0])
                return -1;
        return v[0];
    }

    private static Random rand = new Random();
    public static String randomTitle() {
        StringBuilder sb = new StringBuilder();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < 20; ++i)
            sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        return sb.toString();
    }
}
