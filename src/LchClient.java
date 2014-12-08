import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

class Server {
	public String addr;
	public int port;
	public Server(String addr, int port) {
		this.addr = addr;
		this.port = port;
	}
}

public class LchClient {
	// Meta data
	private final static String lchDir = ".lch/";
	private final String serverListFile = lchDir + "servers";
	private static final String fileMetadataFile = lchDir + "metadata";
	private static HashMap<String, String> fileDigests;
	private static int version = 0;
	
	// <IP, Port>
	private Vector<Server> serverList;
	// Random Generator
	private Random randomGen;;
	
	// NetIO
	private final int port = 7000;
    private volatile NetIO net;
    private int maxNumRetry = 5;
	
	private boolean updateServerList() {
		FileReader fin;
		BufferedReader buf;
		try {
			fin = new FileReader(new File(serverListFile));
			buf = new BufferedReader(fin);
			String line;
			while ((line = buf.readLine()) != null) {
				String [] strs = line.split(":");
				serverList.add(new Server(strs[0], Integer.parseInt(strs[1])));
			}
			buf.close();
			fin.close();
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private static void hashFiles(String fileStr) {
		// ignore file or directory that starts with "."
		if (fileStr.startsWith("./."))
			return;
		
		File file = new File(fileStr);
		if (file.isFile()) {
			String sha1 = HashUtils.genSHA1(file);
			// For test
			//System.out.println(file.getPath() + ": " + sha1);
			fileDigests.put(file.getPath().substring(2), sha1);
		} else if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				hashFiles(f.getPath());
			}
		}
	}
	
	private static void fileHashToFile() {
		try {
			FileOutputStream fos = new FileOutputStream(fileMetadataFile);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeInt(version);
			oos.writeObject(fileDigests);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	private static HashMap<String, String> fileHashFromFile() {
		HashMap<String, String> curFileDigests = null;
		try {
			FileInputStream fis;
			fis = new FileInputStream(fileMetadataFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			//int tmpVersion = 
			ois.readInt();
			curFileDigests = (HashMap<String, String>) ois.readObject();
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			curFileDigests = new HashMap<String, String>();
			//e.printStackTrace();
		}
		return curFileDigests;
	}
	
	
	private String hashFile(String fileStr) {
		return HashUtils.genSHA1(new File(fileStr));
	}
	
	public boolean init() {
		fileDigests = new HashMap<String, String>();
		net = new NetIO(port);
		randomGen = new Random();
		serverList = new Vector<Server>();
		if (!updateServerList())
			return false;

		return true;
	}
	
	private Server pickRandomServer() {
		return serverList.get(randomGen.nextInt(serverList.size()));
	}
	
	public void cleanUp() {
		net.close();
	}
	
	private String genRandomString() {
		long seed = randomGen.nextLong();
		return HashUtils.genMD5(new String(Long.toString(seed)));
	}
	
	public class commitComparator implements Comparator<Commit> {
		public int compare(Commit c1, Commit c2) {
			return c1.commitId - c2.commitId;
		}
	}
	
	private boolean doSync(Command cmd) {
		System.out.println("doSync");
		SyncRequest syncReq = new SyncRequest();
		syncReq.responseTitle = genRandomString();
		Message msg = null;
		int numRetry = 0;
		while (msg == null && (numRetry++) < maxNumRetry) {
			Server server = pickRandomServer();
			net.sendMessage(server.addr, server.port, "SyncRequest", syncReq);
			msg = net.receiveMessage(syncReq.responseTitle, NetIO.numNanosPerSecond * 10);
		}
		if (msg == null) {
			System.err.println("Network is unstable");
			return false;
		}
		SyncResponse syncRes = (SyncResponse) msg.content;
		List<Commit> commits = syncRes.commits;
		Collections.sort(commits, new commitComparator());
		
		return true;
	}
	
	private boolean doCommit(Command cmd) {
		System.out.println("doCommit");
		CommitRequest commitReq = new CommitRequest();
		commitReq.responseTitle = genRandomString();
		
		// Get historical hash value from meta data file
		HashMap<String, String> oldFileDigests = fileHashFromFile();
		hashFiles(".");
		@SuppressWarnings("unchecked")
		HashMap<String, String> curFileDigests = (HashMap<String, String>) fileDigests.clone();

		Commit commit = new Commit();
		commit.author = System.getProperty("user.name");
		if (commit.author == "")
			commit.author = "anonymous";
		commit.nanoTimestamp = System.nanoTime();
		commit.commitId = version + 1;
		commit.removedFiles.addAll((Set<String>) oldFileDigests.keySet());
		commit.removedFiles.removeAll(curFileDigests.keySet());

		Iterator<String> it = curFileDigests.keySet().iterator();
		while (it.hasNext()) {
			String tmpKey = it.next();
			if (oldFileDigests.get(tmpKey) != curFileDigests.get(tmpKey)) {
				Path path = Paths.get(tmpKey);
				try {
					commit.changedFiles.put(tmpKey, Files.readAllBytes(path));
					byte [] bytes = Files.readAllBytes(path);
					System.out.println();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		commitReq.baseCommit = version;
		commitReq.proposedCommit = commit;
		
		Message msg = null;
		int numRetry = 0;
		while (msg == null && (numRetry++) < maxNumRetry) {
			Server server = pickRandomServer();
			net.sendMessage(server.addr, server.port, "CommitRequest", commitReq);
			msg = net.receiveMessage(commitReq.responseTitle, NetIO.numNanosPerSecond * 10);
		}
		if (msg == null) {
			System.err.println("Network is unstable");
			return false;
		}
		CommitResponse commitRes = (CommitResponse) msg.content;
		if (!commitRes.accepted) {
			System.out.println(commitRes.comment);
		} else {
			version ++;
			fileHashToFile();
		}
		return commitRes.accepted;
	}
	
	public void run(Command cmd) {
		switch (cmd.getCmd()) {
		case "sync":
			doSync(cmd);
			break;
		case "commit":
			doCommit(cmd);
			break;

		default:
			break;
		}		
	}
	
	
	public static void main (String [] args) {
		// For test
//		System.out.println(System.getProperty("user.name"));
//		fileDigests = new TreeMap<String, String>();
//		hashFiles(".");
//		fileHashToFile();
//		TreeMap<String, String> fileHashs = fileHashFromFile();
//		System.out.println(fileHashs);
//		System.exit(1);		
//		System.out.println(args.length);
		
		Command cmd = CommandLineParser.parse(args);
		if (cmd == null) {
			CommandLineParser.printHelp();
			System.exit(1);
		}
		//System.out.println("CMD: " + cmd.getCmd() + ", FILE: " + cmd.getFileName());
		//System.exit(0);
		
		LchClient lchClient = new LchClient();
		if (!lchClient.init()) {
			System.err.println("Initialization failed!");
			System.exit(1);
		}
		lchClient.run(cmd);
		lchClient.cleanUp();
	}
}
