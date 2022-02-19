package naming;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;

import rmi.*;
import common.*;
import storage.*;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
	Skeleton<Service> service_skeleton = null;
	Skeleton<Registration> registration_skeleton = null;
	Branch tree;
	ArrayList<Command> commandStubs = null;
	ArrayList<Storage> storageStubs = null;
	
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
    	InetSocketAddress service_address = new InetSocketAddress(NamingStubs.SERVICE_PORT);
		this.service_skeleton = new Skeleton(Service.class, this, service_address);
		InetSocketAddress registration_address = new InetSocketAddress(NamingStubs.REGISTRATION_PORT);
		this.registration_skeleton = new Skeleton(Registration.class, this, registration_address);
		this.tree = new Branch("/");	// Create root node
		storageStubs = new ArrayList<Storage>();
		commandStubs = new ArrayList<Command>();
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
    	try {
			this.service_skeleton.start();
			this.registration_skeleton.start();
		} catch (Exception e) {
			throw new RMIException("Naming Server could not be started");
		}
    }

    /** Stops the naming server.

        <p>
        This method waits for both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
    	if (service_skeleton != null && registration_skeleton != null) {
			this.service_skeleton.stop();
			this.registration_skeleton.stop();
		}

		stopped(null);
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Service.java.
    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
    	if (path == null) {
			throw new NullPointerException();
		}
		if (path.isRoot()) {
			return true;
		}
		/**
		 * Go through the path and check if each components (node) exists, if not throw exception
		 * Checks if the given path is a file. If not, it is a directory.
		 */
		Node dir = this.tree;

		Iterator<String> itr = path.iterator();
		while (itr.hasNext()) {
			String component = itr.next();
			if (getBranch(dir, component) instanceof Leaf) {	// if component is a file, return false
				return false;
			}
			if (getBranch(dir, component) == null) {
				throw new FileNotFoundException("File not found");
			} else
				dir = getBranch(dir, component);
		}
		if (dir instanceof Leaf) {
			return false;
		} else
			return true;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
    	if (directory == null) {
			throw new NullPointerException();
		}
		Branch currDir = this.tree;
		if (directory.name.equals("/")) {
			currDir = this.tree;
			ArrayList<String> contentList = new ArrayList<>();

			for (int i = 0; i < currDir.nodeList.size(); i++) {
				contentList.add(currDir.nodeList.get(i).name);
			}
			String[] contentArr = new String[contentList.size()];
			for (int i = 0; i < contentList.size(); i++) {
				contentArr[i] = contentList.get(i);
			}
			return contentArr;
		} else {	// if path is not root
			Node directoryNode = this.tree;
			Iterator<String> itr = directory.iterator();
			
			while (itr.hasNext()) {	// Finds the node representing the directory
				String component = itr.next();
				if (getBranch(directoryNode, component) == null) {
					throw new FileNotFoundException("File not found");
				}
				if (getBranch(directoryNode, component) instanceof Leaf) {
					throw new FileNotFoundException("File is already present");
				}
				else
					directoryNode = getBranch(directoryNode, component);
			}
			
			ArrayList<String> contentList = new ArrayList<>();
			for (int i = 0; i < ((Branch) directoryNode).nodeList.size(); i++) {
				contentList.add(((Branch) directoryNode).nodeList.get(i).name);
			}
			String[] contentArr = new String[contentList.size()];
			for (int i = 0; i < contentList.size(); i++) {
				contentArr[i] = contentList.get(i);
			}
			return contentArr;
		}
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
    	if (file == null) {
			throw new NullPointerException();
		}
		if (file.isRoot()) {	// Can not create root
			return false;
		}
		if (!isDirectory(file.parent())) {	// Cannot create a file inside a file
			throw new FileNotFoundException();
		}

		/*
		 * If parent is root, and if file doesn't exist add to new leaf (file) to
		 * nodeList of root with storage stub and tell storage server to create file on
		 * its end using command stub
		 */
		if (file.parent().isRoot()) {
			if (getBranch(this.tree, file.last()) == null) {	// if file is not present
				this.tree.nodeList.add(new Leaf(file.last(), commandStubs.get(0), storageStubs.get(0)));
				commandStubs.get(0).create(file);
				return true;
			} else
				return false;	// file is already present
		} else {
			Node currentDir = this.tree;
			Iterator<String> itr = file.parent().iterator();	// Create iterator on the parent path
			
			while (itr.hasNext()) {	// Check if all components/nodes exist
				String component = itr.next();
				if (getBranch((Branch) currentDir, component) == null) {
					throw new FileNotFoundException("Not found");
				}
				currentDir = getBranch(currentDir, component);
			}
			if (currentDir instanceof Leaf) {	// If the current node is a leaf, the return false
				return false;
			}

			/*
			 * If file doesn't exist in current node, add to new leaf (file) to nodeList of
			 * node with storage stub and tell storage server to create file using command stub
			 */
			if (getBranch((Branch) currentDir, file.last()) == null) {
				((Branch) currentDir).nodeList
						.add(new Leaf(file.last(), commandStubs.get(0), storageStubs.get(0)));
				commandStubs.get(0).create(file);
				return true;
			}
			return false;
		}
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
    	if (directory == null) {
			throw new NullPointerException();
		}
		if (directory.isRoot()) {
			return false;
		}
		if (!isDirectory(directory.parent())) {	// Check if parent of directory is a directory
			throw new FileNotFoundException();
		}
		/**
		 * If parent is directory, check if directory exists in the nodeList of root.
		 * If not create new branch and add to nodeList
		 */
		if (directory.parent().isRoot()) {
			if (getBranch((Branch) this.tree, directory.last()) == null) {
				((Branch) this.tree).nodeList.add(new Branch(directory.last()));
				return true;
			}
			else {
				return false;
			}
		}
		Node currDir = this.tree;	// Start at root go to parent directory
		Iterator<String> itr = directory.parent().iterator();

		while (itr.hasNext()) {
			String component = itr.next();
			if (getBranch((Branch) currDir, component) == null) {
				throw new FileNotFoundException("Not found");
			}
			currDir = getBranch(currDir, component);
		}
		if (currDir instanceof Leaf) {	// if current node is leaf, return false
			return false;
		}
		if (getBranch(currDir, directory.last()) == null) {	// If directory does not exists in current node, create new branch and add to nodeList of current node and return true
			((Branch) currDir).nodeList.add(new Branch(directory.last()));
			return true;
		} else
			return false;
    }

    /**
	 * Finds the index of node from a list of nodes using the name of the node
	 * @param list of nodes
	 * @param name	of the node
	 * @return	index of the node
	 */
	public int nodeIndex(ArrayList<Node> list, String name) {
		if (list == null) {
			throw new NullPointerException();
		}
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).name.equals(name)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Finds the node present in the list of node
	 * @param list of nodes
	 * @param name of the node
	 * @return Node
	 */
	public Node getNode(ArrayList<Node> list, String name) {
		if (list == null) {
			throw new NullPointerException();
		}
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).name.equals(name)) {
				return list.get(i);
			}
		}
		return null;
	}

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
    	if (path == null) {
			throw new NullPointerException("File cannot be null");
		}
		if (!exist(path)) {	// Check if path exists
			throw new FileNotFoundException("File does not exist");
		}
		if (path.isRoot()) {
			return false;
		}
		if (path.parent().isRoot()) {	// If parent is root, call deleteUtil with root and file/dir node to be deleted and name of node to be deleted
			Node prev = tree;
			Node curr = getNode(((Branch) prev).nodeList, path.last());
			return deleteUtil(path, prev, curr, curr.name);
		}
		else {	// Else go to the node to be deleted and call the deleteUtil with parent of node, the node itself and the name of the node to be deleted
			Node prev = this.tree;
			Iterator<String> itr = path.parent().iterator();
			while (itr.hasNext()) {
				prev = getBranch(prev, itr.next());
			}
			Node curr = getBranch(prev, path.last());
			return deleteUtil(path, prev, curr, curr.name);
		}
    }
    
    /**
	 * Deletes the file/dir from the directory tree and informs the respective storage server to delete the fil/dir from its server
	 * @param path	
	 * @param prev
	 * @param curr
	 * @param name
	 * @return
	 * @throws FileNotFoundException
	 */
	public synchronized boolean deleteUtil(Path path, Node prev, Node curr, String name)
			throws FileNotFoundException {
		if (!exist(path)) {
			throw new FileNotFoundException("File does not exist");
		}
		if (path.isRoot()) {
			return false;
		}
		if (!isDirectory(path)) {	// If the given path is of a file
			int index_remove = nodeIndex(((Branch) prev).nodeList, name);	// Gets the index of its position in its parents node list
			Node node = getNode(((Branch) prev).nodeList, name);	// Gets the actual node
			try {
				((Leaf) node).command.delete(path);	// Deletes the file from the storage server
			} catch (RMIException e) {}

			if (((Leaf) node).commandList.size() != 0) {	// Deletes the file replicas from the storage server
				for (int i = 0; i < ((Leaf) node).commandList.size(); i++) {
					try {
						((Leaf) node).commandList.get(i).delete(path);
					} catch (RMIException e) {}
				}
			}
			((Branch) prev).nodeList.remove(index_remove);	// Removes the directory tree by removing the node from the parent node list
			return true;
		}
		if (isDirectory(path)) {	// If the given path is of a directory
			for (int i = 0; i < ((Branch) curr).nodeList.size(); i++) {	// Finds the storage server where this directory is located and delegates it to delete the directory
				Node node = getNode(((Branch) curr).nodeList, ((Branch) curr).nodeList.get(i).name);	// Get child node from current node list
				if (node instanceof Leaf) {
					try {
						((Leaf) node).command.delete(path);
					} catch (RMIException e) {}
					if (((Leaf) node).commandList.size() != 0) {	// Delete all replicas on the storage servers
						for (int j = 0; j < ((Leaf) node).commandList.size(); j++) {
							try {
								((Leaf) node).commandList.get(j).delete(path);
							} catch (RMIException e) {}
						}
					}
				}
			}
			int delIndex = nodeIndex(((Branch) prev).nodeList, name);	// Remove the directory by getting the index of its position in its parent node list and removing the node at that index
			((Branch) prev).nodeList.remove(delIndex);
			return true;
		}
		return false;
	}

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
    	if (file == null) {
			throw new NullPointerException();
		}
		if (isDirectory(file)) {	// Cannot fetch storage stub for directory
			throw new FileNotFoundException("Cannot send directories");
		}
		Iterator<String> itr = file.iterator();	// Iterate through the path until reaches a leaf/file)
		Node root = this.tree;
		Node currDir = getBranch(root, itr.next());	// current node
		if (currDir == null) {
			throw new FileNotFoundException();
		}
		while (itr.hasNext()) {	// Checking if all nodes (directories) exist
			currDir = getBranch(currDir, itr.next());
			if (currDir == null) {
				throw new FileNotFoundException();
			}
		}
		return ((Leaf) currDir).storage;
    }
    
    /**
	 * @param root The branch where to find the file
	 * @param name The file to be found
	 * @return	node if the node is present in the branch
	 */
	public static Node getBranch(Node root, String name) {	// For example, tree -> Branch.name = "/", "data"
		Branch temp = (Branch) root;
		ArrayList<Node> list = temp.nodeList;
		// Go through list of all nodes of root, if found then return node
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).name.equals(name)) {
				return (Node) list.get(i);
			}
		}
		return null;
	}

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
    	if (client_stub == null || command_stub == null || files == null) {
			throw new NullPointerException("Null argument found");
		}
		for (int i = 0; i < this.storageStubs.size(); i++) {	// Check if the storage server has already been registered by checking storage stubs in storage stub list
			if (storageStubs.get(i).equals(client_stub)) {
				throw new IllegalStateException("Storage Server already start");
			}
		}
		this.storageStubs.add(client_stub);
		this.commandStubs.add(command_stub);
		ArrayList<Path> duplicates = new ArrayList<>();

		duplicates = createTree(files, client_stub, command_stub);	// Returns a list of duplicates found
		Path[] duplicatesArr = new Path[duplicates.size()];
		for (int i = 0; i < duplicates.size(); i++) {
			duplicatesArr[i] = duplicates.get(i);
		}
		return duplicatesArr;
    }
    
    /**
	 * Returns an array of duplicate files from naming server already been registered
	 * @param files	
	 * @param stub_storage
	 * @param stub_command
	 * @return an array of duplicate files
	 */
	public ArrayList<Path> createTree(Path[] files, Storage storageStub, Command commandStub) {
		ArrayList<Path> duplicates = new ArrayList<>();
		for (int i = 0; i < files.length; i++) {	// Iterate through all the files
			Branch currNode = this.tree;	// assigning root
			Iterator<String> itr = files[i].iterator();
			
			while (itr.hasNext()) {
				String nextComp = itr.next();
				if (itr.hasNext()) {	// If has next, then it is a directory 
					if (currNode.getDirectory(nextComp) != null) {	// If directory already exists, then point to current directory
						currNode = (Branch) currNode.getDirectory(nextComp);
					}
					else {	// If directory does not exist then create a new branch (directory) add branch to nodeList of current directory
						Branch newBranch = new Branch(nextComp);
						currNode.nodeList.add(newBranch);
						currNode = newBranch;
					}

				}
				if (!itr.hasNext()) {	// If does not have next, then its a file
					if (currNode.getDirectory(nextComp) != null) {	// Duplicate file
						duplicates.add(files[i]);
					} else {	// Else create a new leaf (file)
						Leaf newleaf = new Leaf(nextComp, commandStub, storageStub);
						currNode.nodeList.add(newleaf);
					}
				}
			}
		}
		return duplicates;
	}

	/**
	 * Checks if the file/dir given by path exists in the directory tree or not
	 * @param path
	 * @return true if exists, else false
	 */
	public synchronized boolean exist(Path path) {
		if (path.isRoot()) {
			return true;
		}
		Node currDir = this.tree;	// Assigning root
		Iterator<String> itr = path.iterator();
		while (itr.hasNext()) {
			String component = itr.next();
			if (getBranch((Branch) currDir, component) == null) {
				return false;
			}
			currDir = getBranch(currDir, component);
		}
		return true;
	}
}


/** 
 * A branch represents a directory within the directory tree. It contains a list of nodes that has all its immediate directories or files
 */
class Branch extends Node{	
	ArrayList<Node> nodeList;
	
	public Branch(String name) {
		this.name = name;
		this.nodeList = new ArrayList<Node>();
	}
	
	/**
	 * Returns node with "name" from branch nodeList
	 * @param name
	 * @return
	 */
	public Node getDirectory(String name) {
		for (int i = 0; i < nodeList.size(); i++) {
			if (nodeList.get(i).name.equals(name)) {
				return nodeList.get(i);
			}
		}
		return null;
	}
}

/**
 * Leaf represents a file in the directory tree
 */
class Leaf extends Node {

	Command command;	// for accessing the storage server where original file is held
	Storage storage;
	ArrayList<Storage> storageList;	// List of command and storage stubs for accessing the storage servers where the replicas of file are held
	ArrayList<Command> commandList;

	public Leaf(String name, Command commandStub, Storage storageStub) {
		this.name = name;
		this.command = commandStub;
		this.storage = storageStub;
		this.storageList = new ArrayList<Storage>();
		this.commandList = new ArrayList<Command>();
	}
}


/**
 * Node contains all the immediate files or directories for a branch
 *
 */
class Node{	
	String name;
}
