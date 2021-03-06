package rmi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.Arrays;
import java.lang.reflect.*;

/** RMI skeleton

    <p>
    A skeleton encapsulates a multithreaded TCP server. The server's clients are
    intended to be RMI stubs created using the <code>Stub</code> class.

    <p>
    The skeleton class is parametrized by a type variable. This type variable
    should be instantiated with an interface. The skeleton will accept from the
    stub requests for calls to the methods of this interface. It will then
    forward those requests to an object. The object is specified when the
    skeleton is constructed, and must implement the remote interface. Each
    method in the interface should be marked as throwing
    <code>RMIException</code>, in addition to any other exceptions that the user
    desires.

    <p>
    Exceptions may occur at the top level in the listening and service threads.
    The skeleton's response to these exceptions can be customized by deriving
    a class from <code>Skeleton</code> and overriding <code>listen_error</code>
    or <code>service_error</code>.
*/
public class Skeleton<T>
{
		public ServerSocket listenSocket = null;	// Server socket for the skeleton to listen to requests to come in over the network
		public InetSocketAddress SkeletonAddress = null;	// The address at which the skeleton is to run
		public Class<T> ServerInterface = null;	// An object representing the class of the interface for which the skeleton server is to handle method call requests.
		public T ServerImpl = null;	// An object implementing said interface. Requests for method calls are forwarded by the skeleton to this object.
		public boolean isConnected = false;	// checks if skeleton has started
    /** Creates a <code>Skeleton</code> with no initial server address. The
        address will be determined by the system when <code>start</code> is
        called. Equivalent to using <code>Skeleton(null)</code>.

        <p>
        This constructor is for skeletons that will not be used for
        bootstrapping RMI - those that therefore do not require a well-known
        port.

        @param c An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> c, T server) {
		if (c == null) {
			throw new NullPointerException("Null interface found");
		}
		if (!isRemoteInterface(c)) {
			throw new Error("C is not a remote interface");
		}
		if (server == null) {
			throw new NullPointerException("Object implementing interface cannot be null ");
		}
		InetAddress temp = null;
		this.SkeletonAddress = null;
		this.ServerInterface = c;
		this.ServerImpl = server;
    }

    /** Creates a <code>Skeleton</code> with the given initial server address.

        <p>
        This constructor should be used when the port number is significant.

        @param c An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @param address The address at which the skeleton is to run. If
                       <code>null</code>, the address will be chosen by the
                       system when <code>start</code> is called.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> c, T server, InetSocketAddress address)
    {
		if (c == null) {
			throw new NullPointerException("Null interface found");
		}
		if (!isRemoteInterface(c)) {
			throw new Error("C is not a remote interface");
		}
		if (server == null) {
			throw new NullPointerException("Object implementing interface cannot be null ");
		}

		this.ServerInterface = c;
		this.ServerImpl = server;
		this.SkeletonAddress = address;
    }
    
    /*
	 * isRemoteInterface - check to see if all methods in testInterface throw an RMI
	 * exception
	 */

	public boolean isRemoteInterface(Class<T> testInterface) {
		Method[] methods = testInterface.getMethods();
		for (int i = 0; i < methods.length; i++) {
			Class[] exceptions = methods[i].getExceptionTypes();
			String[] exceptionList = new String[exceptions.length];
			for (int j = 0; j < exceptions.length; j++) {
				exceptionList[j] = exceptions[j].toString();
			}
			if ((Arrays.asList(exceptionList).contains("class rmi.RMIException"))) {
				return true;
			}
		}
		return false;
	}

    /** Called when the listening thread exits.

        <p>
        The listening thread may exit due to a top-level exception, or due to a
        call to <code>stop</code>.

        <p>
        When this method is called, the calling thread owns the lock on the
        <code>Skeleton</code> object. Care must be taken to avoid deadlocks when
        calling <code>start</code> or <code>stop</code> from different threads
        during this call.

        <p>
        The default implementation does nothing.

        @param cause The exception that stopped the skeleton, or
                     <code>null</code> if the skeleton stopped normally.
     */
    protected void stopped(Throwable cause)
    {
    }

    /** Called when an exception occurs at the top level in the listening
        thread.

        <p>
        The intent of this method is to allow the user to report exceptions in
        the listening thread to another thread, by a mechanism of the user's
        choosing. The user may also ignore the exceptions. The default
        implementation simply stops the server. The user should not use this
        method to stop the skeleton. The exception will again be provided as the
        argument to <code>stopped</code>, which will be called later.

        @param exception The exception that occurred.
        @return <code>true</code> if the server is to resume accepting
                connections, <code>false</code> if the server is to shut down.
     */
    protected boolean listen_error(Exception exception)
    {
        return false;
    }

    /** Called when an exception occurs at the top level in a service thread.

        <p>
        The default implementation does nothing.

        @param exception The exception that occurred.
     */
    protected void service_error(RMIException exception)
    {
    }

    /** Starts the skeleton server.

        <p>
        A thread is created to listen for connection requests, and the method
        returns immediately. Additional threads are created when connections are
        accepted. The network address used for the server is determined by which
        constructor was used to create the <code>Skeleton</code> object.

        @throws RMIException When the listening socket cannot be created or
                             bound, when the listening thread cannot be created,
                             or when the server has already been started and has
                             not since stopped.
     */
    public synchronized void start() throws RMIException
    {
    	try {
			Thread newListenerThread = new Thread(new Listen(this.SkeletonAddress));
			newListenerThread.start();
		} catch (IOException e) {
			throw new RMIException("Listen thread could not be started");
		}
    }
    
    /**
	 * Class that can be run in a thread to listen to incoming client requests
	 */
    private class Listen implements Runnable {
		private Listen(InetSocketAddress skeletonAddress) throws IOException {	// Create a new listening socket with address given
			try {
				listenSocket = new ServerSocket();
				listenSocket.bind(skeletonAddress);
				SkeletonAddress = (InetSocketAddress) listenSocket.getLocalSocketAddress();
				isConnected = true;
			} catch (Exception e) {}
		}

		public void run() {	// Accept client requests and creates a new service thread for the client
			while (isConnected) {
				Socket serviceSocket;
				try {
					serviceSocket = listenSocket.accept();
					Thread newServiceThread = new Thread(new Service(serviceSocket));
					newServiceThread.start();
				} catch (IOException e) {}
			}
		}
	}
    
    /**
	 * Class that can be run in a thread to service clients
	 */
    private class Service implements Runnable {

		Socket serviceSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		private Service(Socket serviceSocket) throws IOException {
			this.serviceSocket = serviceSocket;
			this.out = new ObjectOutputStream(this.serviceSocket.getOutputStream());
			this.in = new ObjectInputStream(this.serviceSocket.getInputStream());
		}
		
		/*
		 * The service thread reads the method, arg and argtypes from the input stream and gets the method from
		 * the Interface that the skeleton can handle and invokes it. It writes the object into the output stream
		 */
		public void run() {

			try {
				String methodName = (String) (in.readObject());
				Object[] args = (Object[]) (in.readObject());
				Class[] argTypes = (Class[]) (in.readObject());
				Method m = null;
				Object resultSkeleton = null;
				try {m = ServerInterface.getMethod(methodName, argTypes);
				resultSkeleton = m.invoke(ServerImpl, args);	
				} catch (Exception e) {
					resultSkeleton = e;	// If result was a exception, set result to the exception
				}
				out.writeObject(resultSkeleton);

			} catch (Exception e) {}
			finally {	// After the service is completed , close all the streams and sockets
				if (in != null && out != null && serviceSocket != null) {
					try {
						serviceSocket.close();
						out.close();
						in.close();
					} catch (IOException e) {}
				}
			}
		}
	}

    /** Stops the skeleton server, if it is already running.

        <p>
        The listening thread terminates. Threads created to service connections
        may continue running until their invocations of the <code>service</code>
        method return. The server stops at some later time; the method
        <code>stopped</code> is called at that point. The server may then be
        restarted.
     */
    public synchronized void stop()
    {
		isConnected = false;
		if (listenSocket != null) {
			try {
				listenSocket.close();
			} catch (IOException e) {}
		}
		stopped(null);
    }
}
