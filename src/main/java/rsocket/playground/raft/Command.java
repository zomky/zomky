package rsocket.playground.raft;

public enum Command {

    APPEND_ENTRIES("1"),
    REQUEST_VOTE("2");

    Command(String commandNo) {
        this.commandNo = commandNo;
    }

    private final String commandNo;

    public String getCommandNo() {
        return commandNo;
    }

    public static Command fromCommandNo(String commandNo) {
        switch (commandNo) {
            case "1" : return APPEND_ENTRIES;
            case "2" : return REQUEST_VOTE;
            default  : throw new RaftException("");
        }
    }

}
