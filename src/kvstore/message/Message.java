package kvstore.message;

public class Message {
    private String command;
    private String key;
    private String value;

    public Message(String command, String key, String value) {
        this.command = command;
        this.key = key;
        this.value = value;
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
}