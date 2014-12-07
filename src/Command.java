
public class Command {
	private String cmd;
	private String fileName;
	
	public Command(String cmd, String fileName) {
		this.cmd = cmd;
		this.fileName = fileName;
	}
	
	public String getCmd() {
		return cmd;
	}
	
	public void setCmd(String cmd) {
		this.cmd = cmd;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
