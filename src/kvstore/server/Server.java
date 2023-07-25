package kvstore.server;

import com.google.gson.Gson;
import kvstore.message.Message;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Classe do Servidor responsável por armazenar os dados em uma estrutura de dados e
 * atender às solicitações dos clientes.
 * 
 * Projeto realizado para a disciplina Sistemas Distribuídos - UFABC
 * 
 * @author Lucas Henrique Gois de Campos
 */
public class Server {
    private ServerSocket serverSocket;
    private Map<String, String> store;
    private Map<String, Long> timestamps;
    private Map<String, String> serverAddresses;
    private boolean isLeader;
    private static final Gson gson = new Gson();

    /**
     * Construtor da classe Server.
     * @param isLeader Indica se este servidor é o líder do sistema.
     */
    public Server(boolean isLeader) {
        this.isLeader = isLeader;
        store = new HashMap<>();
        timestamps = new HashMap<>();
        serverAddresses = new HashMap<>();
    }
    
    /**
     * Inicia o servidor para receber as conexões dos clientes.
     * @param ipAddress O endereço IP do servidor.
     * @param port A porta em que o servidor irá escutar as conexões.
     * @param leaderIp O endereço IP do líder do sistema.
     * @param leaderPort A porta do líder do sistema.
     */
    public void start(String ipAddress, int port, String leaderIp, int leaderPort) {
        try {
        	// Inicia o servidor socket
            serverSocket = new ServerSocket(port, 10, InetAddress.getByName(ipAddress));
            System.out.println("Seridor começou em " + ipAddress + ":" + port);

            if (isLeader) {
                // Adiciona o líder na tabela
                serverAddresses.put(ipAddress + ":" + port, "Leader");

                //Lê os endereços e portas dos servidores secundários
                Scanner scanner = new Scanner(System.in);
                System.out.print("Digite o número de servidores secundários: ");
                int numSecondaryServers = scanner.nextInt();
                scanner.nextLine(); 

                for (int i = 0; i < numSecondaryServers; i++) {
                    System.out.print("Digite o endereço IP:porta dos servidores secundários " + (i + 1) + " (e.g., localhost:8001): ");
                    String secondaryServerAddress = scanner.nextLine();
                    serverAddresses.put(secondaryServerAddress, "Secondary");
                }
            }

            while (true) {
            	// Aguarda conexões de clientes.
                Socket clientSocket = serverSocket.accept();
                System.out.println("Nova conexão de " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());

                // Cria uma thread para cada cliente conectado para tratar suas requisições.
                ClientHandler clientHandler = new ClientHandler(clientSocket, leaderIp, leaderPort);
                clientHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Método principal para iniciar a execução do servidor.
     * @param args Argumentos da linha de comando.
     */
    public static void main(String[] args) {
    	// Lê o endereço IP e a porta do servidor a partir da entrada do usuário.
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite o endereço IP: ");
        String ipAddress = scanner.nextLine();
        System.out.print("Digite a porta: ");
        int port = scanner.nextInt();
        scanner.nextLine(); 

        System.out.print("Digite o endereço IP do líder: ");
        String leaderIp = scanner.nextLine();
        System.out.print("Digite a porta do líder: ");
        int leaderPort = scanner.nextInt();

        boolean isLeader = false;

        if (ipAddress.equals(leaderIp) && port == leaderPort) {
            isLeader = true;
            System.out.print("Este servidor é o líder! \n");
        }

        // Inicia o servidor com as informações lidas.
        Server server = new Server(isLeader);
        server.start(ipAddress, port, leaderIp, leaderPort);
    }

    /**
     * Classe aninhada ClientHandler para tratar as requisições de cada cliente conectado ao servidor.
     */
    private class ClientHandler extends Thread {
        private Socket clientSocket;
        private String leaderIp;
        private int leaderPort;

        /**
         * Construtor da classe ClientHandler.
         * @param clientSocket O socket referente a conexão com o cliente.
         * @param leaderIp O endereço IP do líder do sistema.
         * @param leaderPort A porta usada pelo líder do sistema.
         */
        public ClientHandler(Socket clientSocket, String leaderIp, int leaderPort) {
            this.clientSocket = clientSocket;
            this.leaderIp = leaderIp;
            this.leaderPort = leaderPort;
        }

        /**
         * Método run para tratar as requisições do cliente.
         */
        public void run() {
            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
            ) {
            	// Lê a mensagem enviada pelo cliente e a converte para o objeto Message usando Gson.
            	String jsonRequest = reader.readLine();
                Message request = gson.fromJson(jsonRequest, Message.class);
                
                String command = request.getCommand();
                String key = request.getKey();
                String value = request.getValue();
                Long timestamp = request.getTimestamp();
                long serverTimestamp = System.currentTimeMillis();

                Message response;

                // Verifica o tipo de comando da mensagem e chama o método apropriado para tratá-la.
                if ("GET".equals(command)) {
                    response = handleGet(key,timestamp);
                    System.out.println("Cliente ["+clientSocket.getInetAddress()+"]:["+clientSocket.getPort()+"] "+command+" key:["+key+"] ts:["+timestamp+"]. Meu ts é ["+response.getTimestamp()+"], portanto devolvendo ["+response.getValue()+"]");
                    
                } else if ("PUT".equals(command)) {
                    if (isLeader) {
                    	System.out.println("Cliente ["+clientSocket.getInetAddress()+"]:["+clientSocket.getPort()+"] "+command+" key:["+key+"] value:["+value+"]");
                    	
                    	ClientHandler.sleep((long)(Math.random() * 10000 + 5000)); // Simulação da latência do servidor
                    	
                    	response = handlePut(key, value, timestamp); // Insere na tabela local
                        Boolean putOk = sendReplication(request); // Replicação
                        
                        if (putOk == true) {                    	
                            System.out.println("Enviando PUT_OK ao Cliente ["+clientSocket.getInetAddress()+"]:["+clientSocket.getPort()+"] da key:["+key+"] ts:["+serverTimestamp+"]");
                        } else {
                        	response = new Message("PUT_ERROR",key,value,timestamp);
                        }                       
                       
                    } else {
                        // Encaminhe a requisição para o líder
                    	System.out.println("Encaminhando PUT key:["+key+"] value:["+value+"]");
                        response = forwardRequestToLeader(jsonRequest);
                    }
                } else if ("REPLICATION".equals(command)) {
                	System.out.println("REPLICATION key:["+key+"] value:["+value+"] ts:["+timestamp+"]");
                    response = handleReplication(key, value, timestamp); 
                } else {
                    response =  new Message("Erro",key, value, timestamp);
                }
                
                // Prepara a resposta e envia de volta ao cliente.
                writer.println(gson.toJson(response));
                
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }        

        /**
         * Método para enviar as mensagens de replicação para os outros servidores secundários.
         * @param request A mensagem de requisição recebida do cliente que será replicada.
         */
        private Boolean sendReplication(Message request) {
        	// Percorre todos os servidores secundários e envia a mensagem de replicação para cada um deles.
        	int serverCounter = 0;
        	
            for (String serverAddress : serverAddresses.keySet()) {
                if (serverAddress.equals(leaderIp + ":" + leaderPort)) {
                    continue; // Pula o líder
                }

                String[] parts = serverAddress.split(":");
                String ipAddress = parts[0];
                int port = Integer.parseInt(parts[1]);

                try (
                        Socket socket = new Socket(ipAddress, port);
                		PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                ) {
                    // Cria a mensagem de REPLICATION
                    Message replicationMessage = new Message("REPLICATION", request.getKey(), request.getValue(),request.getTimestamp());
                    String jsonMessage = gson.toJson(replicationMessage);
                    writer.println(jsonMessage);
                    
        			// Retorna a resposta recebida do servidor.
        			String response = reader.readLine();
        			Message responseJson = gson.fromJson(response, Message.class);
        			
        			if (responseJson.getCommand().equals("REPLICATION_OK")) {
        				serverCounter += 1;       				
        			}
        			
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            if (serverCounter == serverAddresses.size() - 1) {
            	return true;
            } else {
            	 return false;
            }
        }

        /**
         * Método para encaminhar a requisição para o líder do sistema.
         * @param jsonRequest A requisição em formato JSON recebida do cliente.
         * @return A mensagem de resposta do líder ou uma mensagem de erro, caso não seja possível enviar a requisição.
         */
        private Message forwardRequestToLeader(String jsonRequest) {
            try (
                    Socket leaderSocket = new Socket(leaderIp, leaderPort);
                    PrintWriter writer = new PrintWriter(leaderSocket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()))
            ) {
            	// Encaminha a requisição para o líder e aguarda a resposta.
                writer.println(jsonRequest);
                String jsonResponse = reader.readLine();
                // Retorna a resposta recebida do líder.
                return gson.fromJson(jsonResponse,Message.class);
            } catch (IOException e) {
                e.printStackTrace();
                Message request = gson.fromJson(jsonRequest, Message.class);
                String key = request.getKey();
                String value = request.getValue();
                Long timestamp = request.getTimestamp();
                
                Message errorMessage = new Message("Error forwarding request to leader", key, value, timestamp);
                return errorMessage;
            }
        }

        /**
         * Método para tratar as requisições do tipo GET.
         * @param key A chave da requisição GET.
         * @param clientTimestamp O timestamp enviado pelo cliente na requisição GET.
         * @return A mensagem de resposta adequada para a requisição GET.
         */
        private Message handleGet(String key, long clientTimestamp) {
        	
        	// Obtém o valor e o timestamp armazenados no servidor para a chave especificada.
            String value = store.get(key);
            // Caso a chave não tenha um timestamp atrelado, retorna 0
            long serverTimestamp = timestamps.getOrDefault(key, 0L); 

            if (value != null) {
            	// Compara o timestamp do cliente com o timestamp armazenado no servidor e responde de acordo.
                if (serverTimestamp < clientTimestamp) {
                	
                    Message tryOtherServerOrLaterMessage = new Message("TRY_OTHER_SERVER_OR_LATER", key, "TRY_OTHER_SERVER_OR_LATER", serverTimestamp);
                    return tryOtherServerOrLaterMessage;
                    
                } else { // (serverTimestamp >= clientTimestamp)

                    Message getMessage = new Message("GET", key, value, serverTimestamp);
                    return getMessage;
                }
            } else {
                Message getMessage = new Message("NULL", key, "", 0L);
                return getMessage;
            }
        }

        /**
         * Método para tratar as requisições do tipo PUT.
         * @param key A chave da requisição PUT.
         * @param value O valor associado à chave na requisição PUT.
         * @param timestamp O timestamp da requisição PUT.
         * @return A mensagem de resposta para a requisição PUT.
         */
        private Message handlePut(String key, String value, long timestamp) {
        	// Armazena o valor associado à chave no servidor e atualiza o timestamp.
            store.put(key, value);
            timestamps.put(key, timestamp); 
            // Retorna uma mensagem de resposta indicando que a operação foi bem-sucedida.
            Message putMessage = new Message("PUT_OK", key, value, timestamp);
            return putMessage;
        }
        
        /**
         * Método para tratar as mensagens de replicação recebidas do líder do sistema.
         * @param key A chave recebida na mensagem de replicação.
         * @param value O valor associado à chave na mensagem de replicação.
         * @param timestamp O timestamp da mensagem de replicação.
         * @return A mensagem de resposta para a replicação.
         */
        private Message handleReplication(String key, String value, long timestamp) {
        	// Atualiza o valor associado à chave e o timestamp no servidor com os dados recebidos na mensagem de replicação.
            store.put(key, value);
            timestamps.put(key, timestamp); 
            // Retorna uma mensagem de resposta indicando que a replicação foi bem-sucedida.
            Message replicationMessage = new Message("REPLICATION_OK", key, value, timestamp);
            return replicationMessage;
        }
    }
}
