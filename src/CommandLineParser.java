
public class CommandLineParser {
	public static void printHelp() {
		System.out.println("");
	}
	
	static public Command parse(String [] args) {
		if (args.length == 0) {
			System.err.println("Please specify command");
			return null;
		}
		
		if (!(args[0].equals("sync") || args[0].equals("commit"))) {
			System.err.println("Invalid command: " + args[0]);
			return null;
		}
		
		if (args.length > 2) {
			System.err.println("Invalid command format");
			return null;
		}
		
		Command cmd;
		if (args.length == 1)
			cmd = new Command(args[0], null);
		else
			cmd = new Command(args[0], args[1]);
		
		return cmd;
	}
}
