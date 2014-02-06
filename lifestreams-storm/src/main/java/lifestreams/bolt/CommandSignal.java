package lifestreams.bolt;

public class CommandSignal {
	public enum Command{
		SNAPSHOT
	}
	private Command cmd;
	private String target;
	public CommandSignal(Command cmd, String target){
		this.cmd = cmd;
		this.target = target;
	}
	public Command getCmd() {
		return cmd;
	}
	public String getTarget() {
		return target;
	}
}

