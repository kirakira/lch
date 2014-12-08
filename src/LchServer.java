import java.util.*;
import java.io.*;
import java.util.concurrent.locks.*;

public class LchServer {
    private volatile int serverId, highestProposalNumber;
    private volatile NetIO net;
    private List<Commit> updateLog;
    private volatile boolean closing = false;
    private volatile Thread syncThread, commitThread, acceptorLearnerThread;
    private volatile List<String> serverList;
    private int lastPaxosDecision = -1;

    final Lock paxosLock = new ReentrantLock();
    final Condition paxosCondition = paxosLock.newCondition();

    // serverList should include the address:port of the local server
    public LchServer(int port, int serverId, List<String> serverList) {
        this.serverId = serverId;
        this.serverList = serverList;
        highestProposalNumber = 0;
        net = new NetIO(port);
        updateLog = new ArrayList<Commit>();
        restoreState();

        syncThread = new Thread(new SyncHandler());
        commitThread = new Thread(new CommitHandler());
        acceptorLearnerThread = new Thread(new AcceptorLearner());
        syncThread.start();
        commitThread.start();
        acceptorLearnerThread.start();

        Scanner scan = new Scanner(System.in);
        while (true) {
            String s = scan.nextLine();
            if (s.equals("quit")) {
                close();
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
        } catch (InterruptedException e) {
        }
    }

    private class SyncHandler implements Runnable {
        public void run() {
            while (!closing) {
                Message msg = net.receiveMessage("Sync", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof SyncRequest)) {
                    System.err.println("Discarded a malformed sync request");
                    continue;
                }
                SyncRequest req = (SyncRequest) msg.content;
                req.baseCommit = Math.max(0, req.baseCommit);

                SyncResponse response = new SyncResponse();
                response.commits = new ArrayList<Commit>();
                paxosLock.lock();
                try {
                    for (int i = req.baseCommit + 1; i < updateLog.size(); ++i)
                        response.commits.add(updateLog.get(i));
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
            while (!closing) {
                Message msg = net.receiveMessage("Commit", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof CommitRequest)) {
                    System.err.println("Discarded a malformed commit request");
                    continue;
                }
                CommitRequest req = (CommitRequest) msg.content;

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
                    if (prepareReplyMessage == null || !(prepareReplyMessage.content instanceof PaxosMessage))
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
                        continue;
                    }
                    if (updateLog.size() > req.proposedCommit.commitId
                            && req.proposedCommit.commitId >= 0
                            && updateLog.get(req.proposedCommit.commitId).equals(req.proposedCommit)) {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = true;
                        reply.comment = "";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
                        continue;
                    } else {
                        CommitResponse reply = new CommitResponse();
                        reply.accepted = false;
                        reply.comment = "Please sync";
                        net.sendMessage(msg.replyAddress, msg.replyPort, req.responseTitle, reply);
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
        public void run() {
            int lastPromise = -1;
            int lastAcceptNumber = -1;
            Commit lastAccept = null;
            Map<Integer, Integer> acceptedCounter = new HashMap<Integer, Integer>(),
                rejectedCounter = new HashMap<Integer, Integer>();

            while (!closing) {
                Message msg = net.receiveMessage("Paxos", 2 * NetIO.numNanosPerSecond);
                if (msg == null)
                    continue;
                if (!(msg.content instanceof PaxosMessage)) {
                    System.err.println("Discarded malformed paxos message");
                    continue;
                }
                PaxosMessage paxosMessage = (PaxosMessage) msg.content;
                if (paxosMessage.type == PaxosMessage.Type.Prepare) {
                    PaxosMessage reply = new PaxosMessage();
                    if (paxosMessage.proposalNumber > highestProposalNumber) {
                        reply.type = PaxosMessage.Type.Promise;
                        reply.proposalNumber = lastAcceptNumber;
                        reply.commit = lastAccept;
                        lastPromise = paxosMessage.proposalNumber;
                    } else {
                        reply.proposalNumber = paxosMessage.proposalNumber;
                        reply.type = PaxosMessage.Type.RejectPrepare;
                    }
                    net.sendMessage(msg.replyAddress, msg.replyPort, paxosMessage.responseTitle, reply);
                } else if (paxosMessage.type == PaxosMessage.Type.AcceptRequest) {
                    PaxosMessage reply = new PaxosMessage();
                    reply.proposalNumber = paxosMessage.proposalNumber;
                    if (paxosMessage.proposalNumber >= lastPromise) {
                        reply.type = PaxosMessage.Type.Accepted;
                        reply.commit = paxosMessage.commit;
                        lastAcceptNumber = paxosMessage.proposalNumber;
                        lastAccept = paxosMessage.commit;
                    } else {
                        reply.type = PaxosMessage.Type.RejectAcceptRequest;
                    }
                    net.sendMessage(msg.replyAddress, msg.replyPort, paxosMessage.responseTitle, reply);
                    for (String s: serverList)
                        net.sendMessage(s, "Paxos", reply);
                } else if (paxosMessage.type == PaxosMessage.Type.Accepted) {
                    if (!acceptedCounter.containsKey(paxosMessage.proposalNumber))
                        acceptedCounter.put(paxosMessage.proposalNumber, 0);
                    acceptedCounter.put(paxosMessage.proposalNumber,
                            acceptedCounter.get(paxosMessage.proposalNumber) + 1);
                    if (acceptedCounter.get(paxosMessage.proposalNumber) * 2 >
                            serverList.size()) {
                        paxosLock.lock();
                        try {
                            if (updateLog.size() == paxosMessage.commit.commitId)
                                updateLog.add(paxosMessage.commit);
                            lastAcceptNumber = -1;
                            lastAccept = null;
                            lastPaxosDecision = Math.max(lastPaxosDecision, paxosMessage.proposalNumber);
                            paxosCondition.signalAll();
                        } finally {
                            paxosLock.unlock();
                        }
                    }
                } else if (paxosMessage.type == PaxosMessage.Type.RejectAcceptRequest) {
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
