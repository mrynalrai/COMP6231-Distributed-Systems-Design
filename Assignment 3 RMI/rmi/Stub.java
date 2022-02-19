package rmi;

import java.net.*;
import java.util.Arrays;
import java.io.*;
import java.lang.reflect.*;
import java.lang.reflect.Proxy;

/** RMI stub factory.

    <p>
    RMI stubs hide network communication with the remote server and provide a
    simple object-like interface to their users. This class provides methods for
    creating stub objects dynamically, when given pre-defined interfaces.

    <p>
    The network address of the remote server is set when a stub is created, and
    may not be modified afterwards. Two stubs are equal if they implement the
    same interface and carry the same remote server address - and would
    therefore connect to the same skeleton. Stubs are serializable.
 */
public abstract class Stub
{
    /** Creates a stub, given a skeleton with an assigned adress.

        <p>
        The stub is assigned the address of the skeleton. The skeleton must
        either have been created with a fixed address, or else it must have
        already been started.

        <p>
        This method should be used when the stub is created together with the
        skeleton. The stub may then be transmitted over the network to enable
        communication with the skeleton.

        @param c A <code>Class</code> object representing the interface
                 implemented by the remote object.
        @param skeleton The skeleton whose network address is to be used.
        @return The stub created.
        @throws IllegalStateException If the skeleton has not been assigned an
                                      address by the user and has not yet been
                                      started.
        @throws UnknownHostException When the skeleton address is a wildcard and
                                     a port is assigned, but no address can be
                                     found for the local host.
        @throws NullPointerException If any argument is <code>null</code>.
        @throws Error If <code>c</code> does not represent a remote interface
                      - an interface in which each method is marked as throwing
                      <code>RMIException</code>, or if an object implementing
                      this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, Skeleton<T> skeleton)
        throws UnknownHostException
    {
		if (c == null) {
			throw new NullPointerException("Null interface found");
		}
		if (skeleton == null) {
			throw new NullPointerException("Null skeleton found");
		}
		if (skeleton.SkeletonAddress == null) {
			throw new IllegalStateException("Null skeleton address found");
		}
		if (!skeleton.isConnected) {
			throw new IllegalStateException("Skeleton has not been started");
		}
		if (skeleton.SkeletonAddress.isUnresolved()) {	// Check if address can be found on local host
			throw new UnknownHostException("Skeleton Address is unresolved");
		}
		if (!isRemoteInterface(c)) {
			throw new Error("C is not a remote interface");

		}
		T proxyInstance = null;
		try {
			proxyInstance = (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[] { c },
					new ProxyHandler(skeleton.SkeletonAddress));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new Error("Could not create proxy");
		}
		return proxyInstance;
    }

    /** Creates a stub, given a skeleton with an assigned address and a hostname
        which overrides the skeleton's hostname.

        <p>
        The stub is assigned the port of the skeleton and the given hostname.
        The skeleton must either have been started with a fixed port, or else
        it must have been started to receive a system-assigned port, for this
        method to succeed.

        <p>
        This method should be used when the stub is created together with the
        skeleton, but firewalls or private networks prevent the system from
        automatically assigning a valid externally-routable address to the
        skeleton. In this case, the creator of the stub has the option of
        obtaining an externally-routable address by other means, and specifying
        this hostname to this method.

        @param c A <code>Class</code> object representing the interface
                 implemented by the remote object.
        @param skeleton The skeleton whose port is to be used.
        @param hostname The hostname with which the stub will be created.
        @return The stub created.
        @throws IllegalStateException If the skeleton has not been assigned a
                                      port.
        @throws NullPointerException If any argument is <code>null</code>.
        @throws Error If <code>c</code> does not represent a remote interface
                      - an interface in which each method is marked as throwing
                      <code>RMIException</code>, or if an object implementing
                      this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, Skeleton<T> skeleton,
                               String hostname)
    {
    	if (c == null) {
			throw new NullPointerException("Null interface found");
		}
		if (skeleton == null) {
			throw new NullPointerException("Null skeleton found");
		}
		if (hostname == null) {
			throw new NullPointerException("Null hostname found");
		}
		if (!isRemoteInterface(c)) {
			throw new Error("C is not a remote interface");
		}
 
		if (skeleton.SkeletonAddress.getPort() == 0) {	//Check if skeleton address port is 0, if yes then set new skeleton address
			throw new IllegalStateException("Skeleton port not assigned");
		}
		int Port = skeleton.SkeletonAddress.getPort();
		skeleton.SkeletonAddress = new InetSocketAddress(hostname, Port);
		//Create new Proxy instance with the skeleton adddress
		T proxyInstance = null;
		try {
			proxyInstance = (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[] { c },
					new ProxyHandler(skeleton.SkeletonAddress));
		} catch (Exception e) {
			throw new Error("Could not create proxy");
		}
		return proxyInstance;
    }

    /** Creates a stub, given the address of a remote server.

        <p>
        This method should be used primarily when bootstrapping RMI. In this
        case, the server is already running on a remote host but there is
        not necessarily a direct way to obtain an associated stub.

        @param c A <code>Class</code> object representing the interface
                 implemented by the remote object.
        @param address The network address of the remote skeleton.
        @return The stub created.
        @throws NullPointerException If any argument is <code>null</code>.
        @throws Error If <code>c</code> does not represent a remote interface
                      - an interface in which each method is marked as throwing
                      <code>RMIException</code>, or if an object implementing
                      this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, InetSocketAddress address)
    {
    	if (c == null) {
			throw new NullPointerException("Null interface found");
		}
		if (address == null) {
			throw new NullPointerException("Null skeleton found");
		}
		if (!isRemoteInterface(c)) {
			throw new Error("C is not a remote interface");
		}
		T proxyInstance = null;
		try {
			proxyInstance = (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[] { c },
					new ProxyHandler(address));
		} catch (Exception e) {
			throw new Error("Could not create proxy");
		}
		return proxyInstance;
    }
    
    /**
	 * Function checks if interface is a remote interface 
	 * @param <T>
	 * @param testInterface
	 * @return boolean
	 */
	public static <T> boolean isRemoteInterface(Class<T> testInterface) {
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
	
	/**
	 * implements invoke that tries to invoke a method by sending methodname, args, argtypes to the skeleton via the outstream
	 *
	 */
	public static class ProxyHandler implements InvocationHandler, Serializable {

		public InetSocketAddress skeleton_address = null;
		public ProxyHandler(InetSocketAddress address) {
			this.skeleton_address = address;
		}

		/**
         * Processes a method invocation on a proxy instance and returns the result. 
         * This method will be invoked on an invocation handler when a method is invoked on a proxy instance that it is associated with.
         * Invoke method is an idea that you want to invoke a method, with the given arguments. As well as it checks if method given is local, 
         * if yes, executes within the function
		 * else - marshals the required data (methodname, arg, argtypes) and sends it to skeleton
         */
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {			
			String methodname = method.getName();	// If local method, then execute
			Object result = null;
			if (methodname.equals("equals")) {
				result = this.equals(args[0]);
				return result;
			}
			if (methodname.equals("toString")) {
				result = this.toString();
				return result;
			}
			if (methodname.equals("hashCode")) {
				result = this.hashCode();
				return result;
			}
			else {	
				Socket stubSocket = new Socket();	//Creates new socket and binds it to the skeleton address
				try {
					stubSocket.connect(skeleton_address);
				} catch (IOException e2) {}
				ObjectOutputStream outStub = null;
				ObjectInputStream inStub = null;

				try {
					// For marshalling of data
					outStub = new ObjectOutputStream(stubSocket.getOutputStream());
					inStub = new ObjectInputStream(stubSocket.getInputStream());
					//Writes methodname, args and argtypes to output stream
					outStub.writeObject(methodname);
					outStub.writeObject(args);
					outStub.writeObject(method.getParameterTypes());
				
					result = inStub.readObject();	//Gets response back
				} catch (Exception e) {
					throw new RMIException("Error in creating input/output streams", e);
				}
				if (result instanceof Throwable) {	// If invoked on skeleton-end caused an exception, throw that exception	
					throw ((Throwable)result).getCause();
				}
				if (stubSocket != null && inStub != null && outStub != null) {	// Close all sockets and streams - Clean up activity
					try {
						stubSocket.close();
						inStub.close();
						outStub.close();
					} catch (IOException e) {}
				}
			return result;
			}
		}
		
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if(other == null || this == null) {

				return false;
			}
			if (!(other instanceof Proxy)) {
				return false;
			}
			ProxyHandler other_handler = (ProxyHandler) Proxy.getInvocationHandler(other);	//	Creates proxy handler for the other object
			if ((other_handler.skeleton_address.equals(this.skeleton_address))) {
				return true;
			}
			return false;
		}
		
		/**
		 *	Returns hashcode of stub
		 */
		public int hashCode() {
			return skeleton_address.hashCode();

		}
		
		/**
		 *	Returns string representation of stub (the skeleton address)
		 */
		public String toString() {
			String msg = skeleton_address.toString();
			return msg;
		}
	}
}
