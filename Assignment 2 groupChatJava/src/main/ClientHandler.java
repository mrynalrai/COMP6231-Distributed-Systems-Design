package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;

// This class implements Runnable, which is implemented on a class whose instances will be run on different threads
public class ClientHandler implements Runnable {

	// To keep track of all the users and broadcast messages to them
	public static ArrayList<ClientHandler> clientHandlers = new ArrayList<>();
	private Socket socket;
	private BufferedReader bufferedReader;
	private BufferedWriter bufferedWriter;
	private String clientUsername;
	
	public ClientHandler(Socket socket) {
		try {
			this.socket = socket;
			this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));	// What server will send
			this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));	// What client will send
			this.clientUsername = this.bufferedReader.readLine(); // reading a line till the new line character
			clientHandlers.add(this);
			this.broadcastMessage("[SERVER]: " + this.clientUsername + " has entered the chat.");
		} catch (IOException e) {
			this.closeEverything(this.socket, this.bufferedReader, this.bufferedWriter);
		}
	}
	

	/* run() is run on each separate thread
	 */
	@Override
	public void run() {
		String messageFromClient;
		
		while (socket.isConnected()) {
			try {
				// Server will be listening to messages from the client here
				messageFromClient = this.bufferedReader.readLine();
				this.broadcastMessage("[" + this.clientUsername + "]: " + messageFromClient);
			} catch (IOException e) {
				this.closeEverything(this.socket, this.bufferedReader, this.bufferedWriter);
				break;
			}
		}
	}
	
	public void broadcastMessage(String messageToSend) {
		for (ClientHandler clientHandler: clientHandlers) {
			try {
				if (!clientHandler.clientUsername.equals(this.clientUsername)) {
					clientHandler.bufferedWriter.write(messageToSend);
					clientHandler.bufferedWriter.newLine();
					// A buffer will not be sent down in its upward stream unless it is full, so we mean to manually flush it
					clientHandler.bufferedWriter.flush();
				}
			} catch (IOException e) {
				this.closeEverything(this.socket, this.bufferedReader, this.bufferedWriter);
			}
		}
	}
	
	public void removeClientHandler() {
		clientHandlers.remove(this);
		this.broadcastMessage("[SERVER]: " + this.clientUsername + " has left the chat.");
	}
	
	public void closeEverything(Socket socket, BufferedReader bufferedReader, BufferedWriter bufferedWriter) {
		this.removeClientHandler();
		try {
			// With streams, you only need to close the outer wrapper, as the underlying streams are closed when you close the wrapper
			if (this.bufferedReader != null) {
				this.bufferedReader.close();
			}
			if (this.bufferedWriter != null) {
				this.bufferedWriter.close();
			}
			// Closing the socket will also close its input and output streams
			if (this.socket != null) {
				this.socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
