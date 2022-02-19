package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Client {
	private Socket socket;
	private BufferedReader bufferedReader;
	private BufferedWriter bufferedWriter;
	private String username;

	public Client(Socket socket, String username) {
		try {
			this.socket = socket;
			this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));	// What server will send
			this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));	// What client will send
			this.username = username;
		} catch (IOException e) {
			this.closeEverything(this.socket, this.bufferedReader, this.bufferedWriter);
		}
	}
	
	public void sendMessage () {
		try {
			this.bufferedWriter.write(this.username);
			this.bufferedWriter.newLine();
			this.bufferedWriter.flush();
			
			// Get inputs from the console
			Scanner in = new Scanner(System.in);
			while (socket.isConnected()) {
				String messageToSend = in.nextLine();
				this.bufferedWriter.write(this.username + ": " + messageToSend);
				this.bufferedWriter.newLine();
				this.bufferedWriter.flush();
			}
		} catch (IOException e) {
			this.closeEverything(this.socket, this.bufferedReader, this.bufferedWriter);
		}
	}
	
	// A blocking operation
	// Listen for messages from the group
	public void listenForMessage() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				String msgFromGroupChat;
				
				while(socket.isConnected()) {
					try {
						msgFromGroupChat = bufferedReader.readLine();
						System.out.println(msgFromGroupChat);
					} catch (IOException e) {
						closeEverything(socket, bufferedReader, bufferedWriter);
					}
				}
			}
		}).start();
	}

	public void closeEverything(Socket socket, BufferedReader bufferedReader, BufferedWriter bufferedWriter) {
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
	
	public static void main (String args[]) {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please enter your name: ");
		String username = scanner.nextLine();
		
		try {
			Socket socket = new Socket("localhost", 3001);
			Client client = new Client(socket, username);

			/*
			 * Both methods are blocking code.
			 * Because they are running on diff threads, both can be executed
			 */
			client.listenForMessage();
			client.sendMessage();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
