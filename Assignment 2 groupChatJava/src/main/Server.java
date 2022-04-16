package main;

import java.io.IOException;
import java.net.*;

public class Server {

	// ServerSocket listens to clients' incoming requests and create a socket to communicate with them
	private ServerSocket serverSocket; 
	
	public Server(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}
	
	public void initiateServer() {
		try {
			while (!this.serverSocket.isClosed()) {
	
				// accept() is a blocking method, i.e, the code will halt until the client is connected
				// On successful connection, a socket object is returned to connect with the client
				Socket socket = serverSocket.accept();
				System.out.println("A new user has joined the chat.");	// TODO: add name
				
				// Objects of ClientHandler will be used to communicate with the class.
				// This class implements Runnable, which is implemented on a class whose instances will be run on different threads
				ClientHandler clientHandler = new ClientHandler(socket);
				
				// run() from Runnable is overridden and is run on each thread
				
				Thread thread = new Thread(clientHandler);	// new thread for each new user
				/*
				 * start()
                 * Causes this thread to begin execution; the Java Virtual Machine calls the run method of this thread.
                 * The result is that two threads are running concurrently: the current thread (which returns from the call to the start method) and the other thread (which executes its run method).
                 * It is never legal to start a thread more than once. In particular, a thread may not be restarted once it has completed execution.
				 */
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
			
			// ServerSocket class implements server sockets. A server socket waits for requests to come in over the network. 
			// It performs some operation based on that request, and then possibly returns a result to the requester.
			ServerSocket serverSocket = new ServerSocket(3001);
			Server server = new Server(serverSocket);
			server.initiateServer();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}