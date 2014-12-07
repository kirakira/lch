import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;

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
	private static TreeMap<String, String> fileDigests;
	
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
			oos.writeObject(fileDigests);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static TreeMap<String, String> fileHashFromFile() {
		TreeMap<String, String> curFileDigests = null;
		try {
			FileInputStream fis;
			fis = new FileInputStream(fileMetadataFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			curFileDigests = (TreeMap<String, String>) ois.readObject();
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return curFileDigests;
	}
	
	
	private String hashFile(String fileStr) {
		return HashUtils.genSHA1(new File(fileStr));
	}
	
	public boolean init() {
		fileDigests = new TreeMap<String, String>();
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
	
	private boolean doSync(Command cmd) {
		System.out.println("doSync");
		SyncRequest syncReq = new SyncRequest();
		syncReq.responseTitle = "random string";
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
		return true;
	}
	
	private boolean doCommit(Command cmd) {
		System.out.println("doCommit");
		CommitRequest commitReq = new CommitRequest();
		commitReq.responseTitle = "random string";
		Message msg = null;
		int numRetry = 0;
		while (msg == null && (numRetry++) < maxNumRetry) {
			Server server = pickRandomServer();
			net.sendMessage(server.addr, server.port, "SyncRequest", commitReq);
			msg = net.receiveMessage(commitReq.responseTitle, NetIO.numNanosPerSecond * 10);
		}
		if (msg == null) {
			System.err.println("Network is unstable");
			return false;
		}
		CommitResponse commitRes = (CommitResponse) msg.content;
		return true;
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
