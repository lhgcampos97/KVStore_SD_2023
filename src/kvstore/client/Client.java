package kvstore.client;

import com.google.gson.Gson;
import kvstore.message.Message;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

/**
 * Classe do Cliente responsável por enviar solicitações aos servidores e exibir as respostas recebidas.
 * 
 * Projeto realizado para a disciplina Sistemas Distribuídos - UFABC
 * 
 * @author Lucas Henrique Gois de Campos
 */
public class Client {

	private static final Gson gson = new Gson();
	private static Map<String, Long> timestampStore = new HashMap<>(); // Mapa para armazenar pares chave-timestamp

	/**
	 * Método principal para executar o cliente.
	 * @param args Argumentos da linha de comando.
	 */
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);

		System.out.print("Enter the command (INIT): ");
		String initCommand = scanner.nextLine();

		if (initCommand.equalsIgnoreCase("INIT")) {
			// Inicializa os servidores a partir da entrada do usuário.
			String[] serverAddresses = initializeServers(scanner);
			startClient(scanner,serverAddresses);
		} else {
			System.out.println("Invalid command. Client initialization failed.");
		}
	}

	/**
	 * Inicializa os servidores a partir da entrada do usuário.
	 * @param scanner O objeto Scanner para ler a entrada do usuário.
	 * @return Os endereços IP e portas dos servidores disponíveis digitados pelos usuário.
	 */
	private static String[] initializeServers(Scanner scanner) {
		System.out.print("Enter the number of servers: ");
		int numServers = scanner.nextInt();
		scanner.nextLine(); 

		String[] serverAddresses = new String[numServers];

		for (int i = 0; i < numServers; i++) {
			System.out.print("Enter the IP address and port number of server " + (i + 1) + " (e.g., localhost:8000): ");
			serverAddresses[i] = scanner.nextLine();
		}
		
		return serverAddresses;
	}

	/**
	 * Inicia o loop do cliente para solicitar comandos e interagir com os servidores.
	 * @param scanner O objeto Scanner para ler a entrada do usuário.
	 * @param serverAddresses Os endereços IP e portas dos servidores disponíveis.
	 */
	private static void startClient(Scanner scanner, String[] serverAddresses) {
		boolean exit = false;

		do {

			// Trata as solicitações do usuário (PUT ou GET) e exibe as respostas recebidas dos servidores.
			System.out.print("Enter the command (PUT, GET or exit): ");
			String command = scanner.nextLine();

			if (command.equalsIgnoreCase("exit")) {
				exit = true;
			} else if (command.equalsIgnoreCase("PUT")) {

				System.out.print("Enter the key (or type 'exit' to quit): ");
				String key = scanner.nextLine();

				System.out.print("Enter the value: ");
				String value = scanner.nextLine();

				long timestamp = System.currentTimeMillis(); // Armazena o timestamp atual

				Message message = new Message(command, key, value, timestamp);
				String response = sendRequest(serverAddresses, message);
				System.out.println("Response: " + response);

				Message responseJson = gson.fromJson(response, Message.class);
				timestampStore.put(responseJson.getKey(), responseJson.getTimestamp());

			} else if (command.equalsIgnoreCase("GET")) {

				System.out.print("Enter the key (or type 'exit' to quit): ");
				String key = scanner.nextLine();

				// Caso a chave não tenha um timestamp atrelado, retorna 0
				Long timestamp = timestampStore.getOrDefault(key, 0L);

				Message message = new Message(command, key, null, timestamp); 
				String response = sendRequest(serverAddresses, message);
				System.out.println("Response: " + response);

				Message responseJson = gson.fromJson(response, Message.class);
				timestampStore.put(responseJson.getKey(), responseJson.getTimestamp());

			} else {
				System.out.println("Invalid command");
			}

		} while (!exit);
	}

	/**
	 * Método para enviar uma solicitação ao servidor selecionado aleatoriamente.
	 * @param serverAddresses Os endereços IP e portas dos servidores disponíveis.
	 * @param message A mensagem de solicitação a ser enviada ao servidor.
	 * @return A resposta recebida do servidor ou uma mensagem de erro, caso não seja possível conectar ao servidor.
	 */
	private static String sendRequest(String[] serverAddresses, Message message) {
		// Seleciona aleatoriamente um servidor para enviar a solicitação.
		Random random = new Random();
		String randomServerAddress = serverAddresses[random.nextInt(serverAddresses.length)];

		String response = gson.toJson(new Message("Error: No servers available", "", "", 0L));

		String[] parts = randomServerAddress.split(":");
		String ipAddress = parts[0];
		int port = Integer.parseInt(parts[1]);

		// Envia a solicitação ao servidor selecionado e aguarda a resposta.
		try (
				Socket socket = new Socket(ipAddress, port);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
				) {
			String jsonMessage = gson.toJson(message);
			writer.println(jsonMessage);

			// Retorna a resposta recebida do servidor.
			response = reader.readLine();
			return response;
		} catch (IOException e) {
			// Servidor não disponível
		}

		// Ou uma mensagem de erro, caso não seja possível conectar ao servidor.
		return response;
	}

}
