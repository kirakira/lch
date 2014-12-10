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
		}
        //System.out.println("Initial version: " + version);
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
		// First, we copy current filename to hashValue mapping
		fileDigests = fileHashFromFile();
		System.out.println("Current Version: " + version);
		//hashFiles(".");
		@SuppressWarnings("unchecked")
		HashMap<String, String> copyFileDigests = (HashMap<String, String>) fileDigests.clone();
		SyncRequest syncReq = new SyncRequest();
		syncReq.responseTitle = genRandomString();
		syncReq.baseCommit = version;
		System.out.println("Client: version" + version);
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

		if( commits.size()==0 ) {
			System.out.println("No updated files, Sync Finished");
		    return true;
		}
		// Check if a commit is in conflict
		// merge all commits
		for(int i = 1; i < commits.size(); ++i)
			mergeCommits( commits.get(0), commits.get(i) );
		
		// We apply the hash of changed files to the value
		try {
			if( checkConflictAndSync( fileDigests, commits.get(0) ) ) {
				version = commits.get(0).commitId;
				System.err.println("In conflict with version" 
									+ commits.get(0).commitId + " Sync finished to version" + version );
				fileHashToFile();
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
//		// For debug
//		System.out.println("{");		
//		Iterator<String> it = fileDigests.keySet().iterator();
//		while (it.hasNext()) {
//			String key = it.next();
//			System.out.println(key + " -> " + fileDigests.get(key));
//		}
//		System.out.println("}");
		
		System.out.println("Successfully sync to version" + version );
		return true;
	}
	
	/**
	 * merge two commits, follow the order of A to B,
	 * merge results are in A
	 * @param commitA
	 * @param commitB
	 */
	private void mergeCommits(Commit commitA, Commit commitB) {
		commitA.commitId = commitB.commitId;
		for(String filename: commitB.removedFiles) {
			if( commitA.changedFiles.containsKey(filename) ) {
				commitA.changedFiles.remove( filename );
			}
			commitA.removedFiles.add( filename );
		}
		for(String filename: commitB.changedFiles.keySet()) {
			if( commitA.removedFiles.contains(filename) ) {
				commitA.removedFiles.remove(filename);
			}
			commitA.changedFiles.put(filename, commitB.changedFiles.get(filename));
		}
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
	 * @throws IOException 
	 */
	private boolean checkConflictAndSync(HashMap<String, String> fileDigests,
			Commit commit) throws IOException {
		boolean ifConflict = false;
		byte[] emptyArray = {};
		
		// check if all removed files exist in copyFileDigests
		for(String filename : commit.removedFiles) {
			// if this file not exist in hashmap, conflict
			boolean confFile = false;
			Path path = Paths.get(filename);
			if( !fileDigests.containsKey( filename )) {
				reportConflict( filename, 2, emptyArray );
				confFile = true;
			}
			
			// if change of file, then conflict
			if( Files.exists(path) ) {
				String curHashContent = HashUtils.genSHA1(new String(Files.readAllBytes(path)));
				if( Files.exists(path) && !curHashContent.equals(fileDigests.get(filename)) ) {
					reportConflict( filename, 1, emptyArray );
					confFile = true;
				}
			}
			
			// do sync
			if( confFile ) {
				ifConflict = true;
			}
			else {
				Files.deleteIfExists( path );
				fileDigests.remove( filename );
			}
		}
		
		for(String filename : commit.changedFiles.keySet()) {
			Path path = Paths.get(filename);
			boolean confFile = false;
			// if this file not exist in file system, conflict
			if( !Files.exists(path) ) {
				if( fileDigests.containsKey(filename) ) {
					reportConflict( filename, 0, commit.changedFiles.get(filename) );
					ifConflict = true;
					continue;
				}
			}
			else {
			    // if change of file, then conflict
				String curHashContent = HashUtils.genSHA1(new String(Files.readAllBytes(path)));
				String serverHashContent = HashUtils.genSHA1(new String(commit.changedFiles.get(filename)));
					
				if( !curHashContent.equals(fileDigests.get(filename)) 
						&& !curHashContent.equals(serverHashContent) ) {
					reportConflict( filename, 0, commit.changedFiles.get(filename) );
					confFile = true;
				}
			}
			
			// apply changed files
			if( confFile ) {
				ifConflict = true;
			}
			else {
				Files.deleteIfExists( path );
				Files.write(path, commit.changedFiles.get(filename),
						StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				fileDigests.put(filename, HashUtils.genSHA1(new String(commit.changedFiles.get(filename))) );
			}
		}
		
		// maintain the version and metadatafile
		version = commit.commitId;
		fileHashToFile();
		return ifConflict;
	}

	private void reportConflict(String filename, int conflictType, byte[] bs ) {
		// TODO Auto-generated method stub
		if( conflictType==0 ) {
			System.err.println("Conflict " + filename + " Saved as " + filename + ".serverversion");
			Path path = Paths.get(filename+".serverversion");
				try {
					Files.write(path, bs,
							StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		else if( conflictType==2 ) {
			System.err.println("Conflict " + filename + "The file is deleted in server");
		}
		else if( conflictType==3 ) {
			System.err.println("Conflict" + filename + "The file is already deleted, deleted again in server");
		}
	}

	private boolean doCommit(Command cmd) {
		//System.out.println("doCommit");
		CommitRequest commitReq = new CommitRequest();
		commitReq.responseTitle = genRandomString();
		
		// Get historical hash value from meta data file
		HashMap<String, String> oldFileDigests = fileHashFromFile();
		System.out.println("Current Version: " + version);
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
		commit.nanoTimestamp = System.nanoTime();

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
		
		// For debug
//		System.out.println(commit.removedFiles);
//		Iterator<String> it1 = commit.changedFiles.keySet().iterator();
//		System.out.println("changedFiles {");
//		while (it1.hasNext()) {
//			String key = it1.next();
//			System.out.println(key + " -> " + fileDigests.get(key));
//		}
//		System.out.println("}");		
//		
//		System.out.println("Metadata {");
//		HashMap<String, String> old = fileHashFromFile();
//		it1 = old.keySet().iterator();
//		while (it1.hasNext()) {
//			String key = it1.next();
//			System.out.println(key + " -> " + fileDigests.get(key));
//		}
//		System.out.println("}");
//		
//		System.out.println("fileDigests {");
//		it1 = fileDigests.keySet().iterator();
//		while (it1.hasNext()) {
//			String key = it1.next();
//			System.out.println(key + " -> " + fileDigests.get(key));
//		}
//		System.out.println("}");	
//		System.exit(1);
		
		commitReq.baseCommit = version;
		commitReq.proposedCommit = commit;
		
		Message msg = null;
		int numRetry = 0;
		while (msg == null && (numRetry++) < maxNumRetry) {
			Server server = pickRandomServer();
			System.out.print("Commit#" + (version+1) + " Try to connect " + server.addr + ":" + server.port);
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
