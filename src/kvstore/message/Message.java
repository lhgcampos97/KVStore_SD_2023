package kvstore.message;

/**
 * Representa uma mensagem enviada entre o cliente e o servidor contendo informações relevantes
 * para realizar as operações PUT e GET no armazenamento chave-valor distribuído.
 * 
 * Projeto realizado para a disciplina Sistemas Distribuídos - UFABC
 * 
 * @author Lucas Henrique Gois de Campos
 */
public class Message {
    private String command;
    private String key;
    private String value;
    private long timestamp;

    /**
     * Construtor da classe Message.
     * @param command O comando da mensagem
     * @param key A chave associada à mensagem
     * @param value O valor associado à mensagem 
     * @param timestamp O timestamp da mensagem 
     */
    public Message(String command, String key, String value, long timestamp) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    /**
     * Obtém o comando da mensagem.
     * @return O comando da mensagem.
     */
    public String getCommand() {
        return command;
    }

    /**
     * Obtém a chave associada à mensagem.
     * @return A chave da mensagem.
     */
    public String getKey() {
        return key;
    }

    /**
     * Obtém o valor associado à mensagem.
     * @return O valor da mensagem.
     */
    public String getValue() {
        return value;
    }

    /**
     * Obtém o timestamp da mensagem.
     * @return O timestamp da mensagem.
     */
    public long getTimestamp() {
        return timestamp;
    }
}
