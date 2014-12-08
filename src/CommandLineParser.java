
public class CommandLineParser {
	public static void printHelp() {
		System.out.println("");
	}
	
	private static String parseMsg(String msg) {
		if (msg.length() > 1 && msg.charAt(0) == '"' && msg.charAt(msg.length()-1) == '"')
			return msg.substring(0, msg.length()-2);
		return msg;
	}
	
	static public Command parse(String [] args) {
		if (args.length == 0) {
			System.err.println("Please specify command");
			return null;
		}
		
		Command cmd = null;
		String errStr = null;
		String fileStr = null;
		String msgStr = "";
		switch (args[0]) {
		case "sync":
			if (args.length > 2) {
				errStr = "Invalid command format";
				break;
			}
			cmd = new Command(args[0]);
			if (args.length == 2)
				cmd.setFileName(args[1]);
			break;
		case "commit":
			if (args.length == 2) {
				if (args[1].equals("-m") || args[1].equals("--message"))
					errStr = "Invalid option format: -m";
				else 
					fileStr = args[1];
			} else if (args.length == 3) {
				if (args[1].equals("-m") || args[1].equals("--message"))
					msgStr = parseMsg(args[2]);
				else
					errStr = "Invalid command format";
			} else if (args.length == 4) {
				if (args[1].equals("-m") || args[1].equals("--message")) {
					msgStr = parseMsg(args[2]);
					fileStr = args[3];
				} else if (args[2].equals("-m") || args[2].equals("--message")) {
					fileStr = args[1];
					msgStr = parseMsg(args[3]);
				} else {
					errStr = "Invalid command format";
				}
			} else if (args.length > 4) {
				errStr = "Invalid command format";
				break;
			}
			if (errStr == null) {
				cmd = new Command(args[0]);
				cmd.setMsg(msgStr);
				if (fileStr != null)
					cmd.setFileName(fileStr);
			} else {
				System.err.println(errStr);
			}
			break;
		default:
			System.err.println("Invalid command: " + args[0]);
		}
		return cmd;
	}
}
