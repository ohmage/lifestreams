package lifestreams.bolts;

public class Command {
	public enum CommandType {
		SNAPSHOT
	}

	private CommandType cmd;
	private String target;

	public Command(CommandType cmd, String target) {
		this.cmd = cmd;
		this.target = target;
	}

	public CommandType getCmd() {
		return cmd;
	}

	public String getTarget() {
		return target;
	}
}
