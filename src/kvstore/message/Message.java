package kvstore.message;

public class Message {
    private String command;
    private String key;
    private String value;
    private long timestamp;

    public Message(String command, String key, String value, long timestamp) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getCommand() {
        return command;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
