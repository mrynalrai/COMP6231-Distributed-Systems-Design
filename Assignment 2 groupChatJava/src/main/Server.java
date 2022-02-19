package main;

import java.io.IOException;
import java.net.*;

public class Server {

	// serverSocket will listen to clients' incoming requests and create a socket to communicate with them
	private ServerSocket serverSocket; 
	
	public Server(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}
	
	public void initiateServer() {
		try {
			while (!this.serverSocket.isClosed()) {
	
				// accept() to help connect with client
				Socket socket = serverSocket.accept();
				System.out.println("A new user has joined the chat.");
				
				// Objects of ClientHandler will be used to communicate with the class
				ClientHandler clientHandler = new ClientHandler(socket);
				
				Thread thread = new Thread(clientHandler);	// new thread for each new user
				thread.start(); // runs the thread (and ClientHandler class)
								
			}
		} catch (IOException e) {
			this.terminateServerSocket();
		}
	}
	
	public void terminateServerSocket() {
		try {
			if (this.serverSocket != null) {
				this.serverSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		try {
			
			// Creates a server socket, bound to the port 3001
			ServerSocket serverSocket = new ServerSocket(3001);
			Server server = new Server(serverSocket);
			server.initiateServer();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}