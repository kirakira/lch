import java.util.*;
import java.io.*;
import java.util.concurrent.locks.*;

public class LchServer {
    private volatile int serverId, highestProposalNumber;
    private volatile NetIO net;
    private List<Commit> updateLog;
    private volatile boolean closing = false;
    private volatile Thread syncThread, commitThread,
            acceptorLearnerThread, updateLogRequestThread;
    private volatile List<String> serverList;
    private int lastPaxosDecision = -1;
    private volatile Set<Integer> paxosLearned = new HashSet<Integer>();

    final Lock paxosLock = new ReentrantLock();
    final Condition paxosCondition = paxosLock.newCondition();

    // serverList should include the address:port of the local server
    public LchServer(int port, int serverId, List<String> serverList, boolean catchupMode) {
        this.serverId = serverId;
        this.serverList = serverList;
        highestProposalNumber = 0;
        net = new NetIO(port);
        updateLog = new ArrayList<Commit>();
        restoreState();
        if (catchupMode)
            catchUp();

        syncThread = new Thread(new SyncHandler());
        commitThread = new Thread(new CommitHandler());
        acceptorLearnerThread = new Thread(new AcceptorLearner(false));
        updateLogRequestThread = new Thread(new UpdateLogRequestHandler());
        syncThread.start();
        commitThread.start();
        acceptorLearnerThread.start();
        updateLogRequestThread.start();
    }

    public static final void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int serverId = Integer.parseInt(args[1]);
        List<String> serverList = new ArrayList<String>();
        for (String s: args[2].split(","))
            serverList.add(s);
        boolean catchupMode = true;
        if (args.length >= 4 && args[3].equals("n"))
            catchupMode = false;

        LchServer server = new LchServer(port, serverId, serverList, catchupMode);
        Scanner scan = new Scanner(System.in);
        System.out.println("Server started on port " + port);
        while (true) {
            System.out.flush();
            String s = scan.nextLine();
            if (s.equals("quit")) {
                server.close();
                break;
            }
        }
    }

    private void restoreState() {
        paxosLock.lock();
        try {
            updateLog.add(new Commit());
        } finally {
            paxosLock.unlock();
        }
    }

    public void close() {
        closing = true;
        try {
            syncThread.join();
            commitThread.join();
            acceptorLearnerThread.join();
            updateLogRequestThread.join();
            net.close();
        } catch (InterruptedException e) {
        }
    }

    private void mergeCommits(List<Commit> commits1, List<Commit> commits2) {
        for (Commit c: commits2) {
            if (commits1.get(commits1.size() - 1).commitId < c.commitId) {
                commits1.add(c);
                continue;
            }
            for (int i = 0; i < commits1.size(); ++i) {
                if (commits1.get(i).commitId == c.commitId) {
                    commits1.set(i, c);
                    break;
                } else if (commits1.get(i).commitId > c.commitId) {
                    commits1.add(i, c);
                    break;
                }
            }
        }
    }

    private boolean catchUpdateLog() {
        for (String s: serverList) {
            UpdateLogRequest req = new UpdateLogRequest();
            req.responseTitle = randomTitle();
            req.baseCommit = updateLog.size();
            net.sendMessage(s, "UpdateLog", req);
            Message ret = net.receiveMessage(req.responseTitle, 5 * NetIO.numNanosPerSecond);
            if (ret == null)
                continue;
            if (!(ret.content instanceof List))
                continue;
            @SuppressWarnings("unchecked")
            List<Commit> commits = (List<Commit>) ret.content;
            if (commits.size() > 0 && commits.get(0).commitId > updateLog.size())
                continue;
            paxosLock.lock();
            try {
                mergeCommits(updateLog, commits);
            } finally {
                paxosLock.unlock();
            }
            return true;
        }
        return false;
    }

    private void catchUp() {
        System.out.println("Start in catch-up mode");
        if (!catchUpdateLog())
            System.err.println("Warning: catch upate log failed");
        else
            System.out.println("Step 1 finished");
        new AcceptorLearner(true).run();
        System.out.println("Step 2 finished");
        if (!catchUpdateLog())
            System.err.println("Warning: catch upate log 2 failed");
        System.out.println("Step 3 finished");
    }

    private class UpdateLogRequestHandler implements Runnable {
        public void run() {
            while (!closing) {
                Message msg = net.receiveMessage("UpdateLog", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof UpdateLogRequest))
                    continue;

                UpdateLogRequest req = (UpdateLogRequest) msg.content;
                paxosLock.lock();
                try {
                    ArrayList<Commit> response = new ArrayList<Commit>();
                    for (int i = Math.max(0, req.baseCommit + 1); i < updateLog.size(); ++i)
                        response.add(updateLog.get(i));
                    net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, response);
                } finally {
                    paxosLock.unlock();
                }
            }
        }
    }

    private static class UpdateLogRequest implements Serializable {
        static final long serialVersionUID = 8306525401762584161L;

        public String responseTitle;
        int baseCommit;
    }

    private class SyncHandler implements Runnable {
        public void run() {
            while (!closing) {
                Message msg = net.receiveMessage("SyncRequest", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof SyncRequest)) {
                    System.err.println("Discarded a malformed sync request");
                    continue;
                }
                SyncRequest req = (SyncRequest) msg.content;
                req.baseCommit = Math.max(0, req.baseCommit);

                System.out.println("Received a sync request: " + req.toString());

                SyncResponse response = new SyncResponse();
                response.commits = new ArrayList<Commit>();
                paxosLock.lock();
                try {
                    int cnt = 0;
                    for (int i = req.baseCommit + 1; i < updateLog.size(); ++i) {
                        response.commits.add(updateLog.get(i));
                        ++cnt;
                    }
                    System.out.println("returned " + cnt + " updates");
                } finally {
                    paxosLock.unlock();
                }
                net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, response);
            }
        }
    }

    private String randomTitle() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < 20; ++i)
            sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        return sb.toString();
    }

    private class CommitHandler implements Runnable {
        public void run() {
outerloop:
            while (!closing) {
                Message msg = net.receiveMessage("CommitRequest", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof CommitRequest)) {
                    System.err.println("Discarded a malformed commit request");
                    continue;
                }
                CommitRequest req = (CommitRequest) msg.content;

                System.out.println("Received commit request: " + req.toString());

                int proposalNumber = selectProposeNumber();
                PaxosMessage prepare = new PaxosMessage();
                prepare.type = PaxosMessage.Type.Prepare;
                prepare.proposalNumber = proposalNumber;
                prepare.responseTitle = randomTitle();
                for (String host: serverList)
                    net.sendMessage(host, "Paxos", prepare);
                int promise = 0, rejectPrepare = 0;
                Commit commit = null;
                int highestCommit = -1;
                while (promise * 2 <= serverList.size() && rejectPrepare * 2 <= serverList.size()) {
                    Message prepareReplyMessage = net.receiveMessage(prepare.responseTitle, 10 * NetIO.numNanosPerSecond);
                    if (prepareReplyMessage == null) {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = false;
                        reply.comment = "Paxos prepare timed out";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                        System.out.println("Prepare timed out");
                        continue outerloop;
                    }
                    if (!(prepareReplyMessage.content instanceof PaxosMessage))
                        continue;
                    PaxosMessage respond = (PaxosMessage) prepareReplyMessage.content;
                    if (respond.type == PaxosMessage.Type.Promise) {
                        ++promise;
                        if (respond.proposalNumber > highestCommit) {
                            highestCommit = respond.proposalNumber;
                            commit = respond.commit;
                        }
                    } else if (respond.type == PaxosMessage.Type.RejectPrepare)
                        ++rejectPrepare;
                }
                if (promise * 2 <= serverList.size()) {
                    CommitResponse reply = new CommitResponse();
                    reply.accepted = false;
                    reply.comment = "Paxos prepare message was rejected";
                    net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                    continue;
                }

                PaxosMessage acceptRequest = new PaxosMessage();
                acceptRequest.type = PaxosMessage.Type.AcceptRequest;
                acceptRequest.proposalNumber = proposalNumber;
                acceptRequest.responseTitle = randomTitle();
                if (commit == null)
                    acceptRequest.commit = req.proposedCommit;
                else
                    acceptRequest.commit = commit;
                for (String host: serverList)
                    net.sendMessage(host, "Paxos", acceptRequest);

                paxosLock.lock();
                try {
                    long timeout = 30 * NetIO.numNanosPerSecond;
                    try {
                        while (timeout > 0 && lastPaxosDecision < proposalNumber)
                            timeout = paxosCondition.awaitNanos(timeout);
                    } catch (InterruptedException e) {
                    }
                    if (timeout <= 0) {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = false;
                        reply.comment = "Paxos round timed out";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                        System.out.println("commit rejected due to paxos failure");
                        continue;
                    }
                    if (updateLog.size() > req.proposedCommit.commitId
                            && req.proposedCommit.commitId >= 0
                            && updateLog.get(req.proposedCommit.commitId).equals(req.proposedCommit)) {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = true;
                        reply.comment = "";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                        System.out.println("commit accepted");
                        continue;
                    } else {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = false;
                        reply.comment = "Please sync";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                        System.out.println("commit rejcted due to out of date repo");
                        continue;
                    }
                } finally {
                    paxosLock.unlock();
                }
            }
        }
    }

    private int selectProposeNumber() {
        int ret;
        synchronized (this) {
            ret = highestProposalNumber / serverList.size() * serverList.size() + serverId;
            if (ret <= highestProposalNumber)
                ret += serverList.size();
        }
        return ret;
    }

    private class AcceptorLearner implements Runnable {
        boolean observerMode = false;

        public AcceptorLearner(boolean ob) {
            observerMode = ob;
        }

        public void run() {
            int lastPromise = -1;
            int lastAcceptNumber = -1;
            Commit lastAccept = null;
            Map<Integer, Integer> acceptedCounter = new HashMap<Integer, Integer>(),
                rejectedCounter = new HashMap<Integer, Integer>();
            boolean finishOneRound = false;

            while (!closing && (!observerMode || !finishOneRound)) {
                Message msg = net.receiveMessage("Paxos", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof PaxosMessage)) {
                    System.err.println("Discarded malformed paxos message");
                    continue;
                }
                PaxosMessage paxosMessage = (PaxosMessage) msg.content;
                if (paxosMessage.type == PaxosMessage.Type.Prepare) {
                    System.out.println("Prepare received with proposal number " + paxosMessage.proposalNumber);
                    if (!observerMode) {
                        PaxosMessage reply = new PaxosMessage();
                        if (paxosMessage.proposalNumber > highestProposalNumber) {
                            reply.type = PaxosMessage.Type.Promise;
                            reply.proposalNumber = lastAcceptNumber;
                            reply.commit = lastAccept;
                            lastPromise = paxosMessage.proposalNumber;
                            System.out.println("Promise prepare");
                        } else {
                            reply.proposalNumber = paxosMessage.proposalNumber;
                            reply.type = PaxosMessage.Type.RejectPrepare;
                            System.out.println("Rejected prepare");
                        }
                        net.sendMessage(msg.replyAddress, msg.replyPort, paxosMessage.responseTitle, reply);
                    }
                } else if (paxosMessage.type == PaxosMessage.Type.AcceptRequest) {
                    System.out.println("Accept request received with proposal number " + paxosMessage.proposalNumber
                            + ", " + paxosMessage.commit.toString());
                    if (!observerMode) {
                        PaxosMessage reply = new PaxosMessage();
                        reply.proposalNumber = paxosMessage.proposalNumber;
                        if (paxosMessage.proposalNumber >= lastPromise) {
                            reply.type = PaxosMessage.Type.Accepted;
                            reply.commit = paxosMessage.commit;
                            lastAcceptNumber = paxosMessage.proposalNumber;
                            lastAccept = paxosMessage.commit;
                            System.out.println("Accepted accept request");
                        } else {
                            reply.type = PaxosMessage.Type.RejectAcceptRequest;
                            System.out.println("Rejected accept request");
                        }
                        net.sendMessage(msg.replyAddress, msg.replyPort, paxosMessage.responseTitle, reply);
                        for (String s: serverList)
                            net.sendMessage(s, "Paxos", reply);
                    }
                } else if (paxosMessage.type == PaxosMessage.Type.Accepted) {
                    System.out.println("One accept vote for proposal " + paxosMessage.proposalNumber);
                    if (!acceptedCounter.containsKey(paxosMessage.proposalNumber))
                        acceptedCounter.put(paxosMessage.proposalNumber, 0);
                    acceptedCounter.put(paxosMessage.proposalNumber,
                            acceptedCounter.get(paxosMessage.proposalNumber) + 1);
                    if (acceptedCounter.get(paxosMessage.proposalNumber) * 2 >
                            serverList.size() && !paxosLearned.contains(paxosMessage.proposalNumber)) {
                        finishOneRound = true;
                        paxosLock.lock();
                        System.out.println("Learning proposal " + paxosMessage.proposalNumber);
                        paxosLearned.add(paxosMessage.proposalNumber);
                        try {
                            if ((!observerMode && updateLog.size() == paxosMessage.commit.commitId)
                                    || (observerMode && updateLog.get(updateLog.size() - 1).commitId < paxosMessage.commit.commitId)) {
                                updateLog.add(paxosMessage.commit);
                                System.out.println("Written to update log");
                            } else {
                                System.out.println("Not written to update log");
                            }
                            lastAcceptNumber = -1;
                            lastAccept = null;
                            lastPaxosDecision = Math.max(lastPaxosDecision, paxosMessage.proposalNumber);
                            paxosCondition.signalAll();
                        } finally {
                            paxosLock.unlock();
                        }
                    }
                } else if (paxosMessage.type == PaxosMessage.Type.RejectAcceptRequest) {
                    System.out.println("One reject vote for proposal " + paxosMessage.proposalNumber);
                    if (!rejectedCounter.containsKey(paxosMessage.proposalNumber))
                        rejectedCounter.put(paxosMessage.proposalNumber, 0);
                    rejectedCounter.put(paxosMessage.proposalNumber,
                            rejectedCounter.get(paxosMessage.proposalNumber) + 1);
                    if (rejectedCounter.get(paxosMessage.proposalNumber) * 2 >
                            serverList.size()) {
                        paxosLock.lock();
                        try {
                            lastPaxosDecision = Math.max(lastPaxosDecision, paxosMessage.proposalNumber);
                            paxosCondition.signalAll();
                        } finally {
                            paxosLock.unlock();
                        }
                    }
                }

                synchronized (this) {
                    highestProposalNumber = Math.max(highestProposalNumber, paxosMessage.proposalNumber);
                }
            }
        }
    }

    private static class PaxosMessage implements Serializable {
        private static final long serialVersionUID = -2255362036241877525L;

        private static enum Type {
            Prepare,
            Promise,
            RejectPrepare,
            AcceptRequest,
            Accepted,
            RejectAcceptRequest
        }
        public Type type;
        public int proposalNumber;
        public Commit commit;
        public String responseTitle;
    }
}
