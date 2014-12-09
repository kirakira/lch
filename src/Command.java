
public class Command {
	private String cmd;
	private String fileName;
	private String msg;
	
	public Command(String cmd) {
		this.cmd = cmd;
		this.fileName = null;
		this.setMsg("");
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

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
	
	public String toString() {
		return "CMD: " + cmd + " MSG: " + msg + " FILE: " + fileName;
	}
}
