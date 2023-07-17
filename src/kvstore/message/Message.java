package kvstore.message;

import java.io.Serializable;

//Classe que representa uma mensagem
public class Message implements Serializable {
 /**
	 * 
	 */
	private static final long serialVersionUID = 2679759082067357501L;
private String key;
 private String value;

 public Message(String key, String value) {
     this.key = key;
     this.value = value;
 }

 public String getKey() {
     return key;
 }

 public String getValue() {
     return value;
 }
}