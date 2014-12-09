import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
	private final String lchDir = ".lch/";
	private final String serverListFile = lchDir + "servers";
	private final String fileMetadataFile = lchDir + "metadata";
	private HashMap<String, String> fileDigests;
	private int version = 0;
	
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

	private void hashFiles(String fileStr) {
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
	
	private void fileHashToFile() {
		try {
			// overwrite metadata file
			FileOutputStream fos = new FileOutputStream(fileMetadataFile, false);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeInt(version);
			oos.writeObject(fileDigests);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	private HashMap<String, String> fileHashFromFile() {
		HashMap<String, String> curFileDigests = null;
		try {
			FileInputStream fis;
			fis = new FileInputStream(fileMetadataFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			version = ois.readInt();
			curFileDigests = (HashMap<String, String>) ois.readObject();
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			version = 0;
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

		// Check if a commit is in conflict
		// First, we copy current filename to hashValue mapping
		HashMap<String, String> oldFileDigests = fileHashFromFile();
		hashFiles(".");
		@SuppressWarnings("unchecked")
		HashMap<String, String> copyFileDigests = (HashMap<String, String>) fileDigests.clone();
		// We apply the hash of changed files to the value
		for(int i = 0; i < commits.size(); ++i) {
			if( isConflict( copyFileDigests, commits.get(i) ) ) {
				System.err.println("In conflict with version" 
									+ commits.get(i).commitId + " Sync terminated" );
				return false;
			}
		}
		
		// No conflict, apply each commit
		for(int i = 0; i < commits.size(); ++i) {
			syncOneCommit( fileDigests, commits.get(i) );
		}
		System.out.println("Successfully committed to version" + commits.get(commits.size()-1).commitId );
		return true;
	}
	
	/**
	 * Apply the commit to current file system with change of hashvalue
	 * @param fileDigests2
	 * mapping from filename to content hash
	 * @param commit
	 * one commit
	 */
	private void syncOneCommit(HashMap<String, String> fileDigests2,
			Commit commit) {
		// remove these files
		for(String filename : commit.removedFiles) {
			try {
				Path path = Paths.get(filename);
				Files.deleteIfExists( path );
				fileDigests2.remove( filename );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// apply changed files into copyFileDigests
		for(String filename : commit.changedFiles.keySet()) {
			Path path = Paths.get(filename);
			try {
				Files.deleteIfExists( path );
				Files.write(path, commit.changedFiles.get(filename),
						StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				fileDigests2.put(filename, HashUtils.genSHA1(new String(commit.changedFiles.get(filename))) );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// maintain the version and metadatafile
		version = commit.commitId;
		fileHashToFile();
	}

	/**
	 * Check if the commit is in conflict with the hash values of existed files
	 * That equals to check if the removed & changed files exist in current version
	 * @param copyFileDigests 
	 * The mapping from filenames to hash value of file content
	 * @commit
	 * The commit
	 * @return 
	 * true if conflict, otherwise false
	 */
	private boolean isConflict(HashMap<String, String> copyFileDigests,
			Commit commit) {
		// check if all removed files exist in copyFileDigests
		for(String filename : commit.removedFiles) {
			// if this file not exist in hashmap, conflict
			if( !copyFileDigests.containsKey( filename )) {
				reportConflict( filename );
				return true;
			}
			Path path = Paths.get(filename);
			// if this file not exist in file system, conflict
			if( !Files.exists(path) ) {
				reportConflict( filename );
				return true;
			}
			// if change of file, then conflict
			try {
				String curHashContent = HashUtils.genSHA1(new String(Files.readAllBytes(path)));
				if( !curHashContent.equals(copyFileDigests.get(filename)) ) {
					reportConflict( filename );
					return true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		for(String filename : commit.changedFiles.keySet()) {
			// if this file not exist in hashmap, conflict
			if( !copyFileDigests.containsKey( filename )) {
				reportConflict( filename );
				return true;
			}
			Path path = Paths.get(filename);
			// if this file not exist in file system, conflict
			if( !Files.exists(path) ) {
				reportConflict( filename );
				return true;
			}
			// if change of file, then conflict
			try {
				String curHashContent = HashUtils.genSHA1(new String(Files.readAllBytes(path)));
				if( !curHashContent.equals(copyFileDigests.get(filename)) ) {
					reportConflict( filename );
					return true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	private void reportConflict(String filename) {
		// TODO Auto-generated method stub
		System.err.println("Conflict " + filename);
	}

	private boolean doCommit(Command cmd) {
		//System.out.println("doCommit");
		CommitRequest commitReq = new CommitRequest();
		commitReq.responseTitle = genRandomString();
		
		// Get historical hash value from meta data file
		HashMap<String, String> oldFileDigests = fileHashFromFile();
		hashFiles(".");
		@SuppressWarnings("unchecked")
		HashMap<String, String> curFileDigests = (HashMap<String, String>) fileDigests.clone();

		Commit commit = new Commit();
		commit.author = System.getProperty("user.name");
		if (commit.author.equals(""))
			commit.author = "anonymous";
		commit.message = cmd.getMsg();
		commit.commitId = version + 1;
		commit.removedFiles.addAll(oldFileDigests.keySet());
		commit.removedFiles.removeAll(curFileDigests.keySet());

		Iterator<String> it = curFileDigests.keySet().iterator();
		while (it.hasNext()) {
			String tmpKey = it.next();
			if (!curFileDigests.get(tmpKey).equals(oldFileDigests.get(tmpKey))) {
				Path path = Paths.get(tmpKey);
				try {
					commit.changedFiles.put(tmpKey, Files.readAllBytes(path));
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
			System.out.print("Commit#" + version+1 + " Try to connect " + server.addr + ":" + server.port);
			net.sendMessage(server.addr, server.port, "CommitRequest", commitReq);
			msg = net.receiveMessage(commitReq.responseTitle, NetIO.numNanosPerSecond * 10);
			if (msg == null)
				System.out.println("...Failed");
			else
				System.out.println("...Success");
		}
		if (msg == null) {
			System.err.println("Network is unstable");
			return false;
		}
		CommitResponse commitRes = (CommitResponse) msg.content;
		if (!commitRes.accepted) {
			System.out.println("COMMIT FAILED Comments: " + commitRes.comment);
		} else {
			version ++;
			fileHashToFile();
			System.out.println("COMMIT SUCCESS");
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
		Command cmd = CommandLineParser.parse(args);
		if (cmd == null) {
			CommandLineParser.printHelp();
			System.exit(1);
		}
		
		LchClient lchClient = new LchClient();
		if (!lchClient.init()) {
			System.err.println("Initialization failed!");
			System.exit(1);
		}
		lchClient.run(cmd);
		lchClient.cleanUp();
	}
}
