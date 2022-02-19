package storage;

import java.io.*;
import java.net.*;
import java.util.Iterator;

import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
	Skeleton<Storage> storageSkeleton = null;
	Skeleton<Command> commandSkeleton = null;
	File root;
	
    /** Creates a storage server, given a directory on the local filesystem.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root)
    {
    	this(root, 0, 0);
    }
    
    public StorageServer(File root, int clientPort, int commPort) {	// For example, a StorageServer is created by StorageServerApp when it runs
		/**
    	 * For example: 
    	 * root -> storage-test/ (According launcher)
    	 * clientPort -> null
    	 * commPort -> null
    	 */
		if (root == null) {
			throw new NullPointerException("Error: Null directory");
		}
		InetSocketAddress clientAddress = new InetSocketAddress(clientPort);
		InetSocketAddress commandAddress = new InetSocketAddress(commPort);

		this.storageSkeleton = new Skeleton<Storage>(Storage.class, this, clientAddress);
		this.commandSkeleton = new Skeleton<Command>(Command.class, this, commandAddress);

		this.root = root;

	}

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
    	if (hostname == null || naming_server == null) {
			throw new NullPointerException("Arguments cannot be null");
		}
		this.storageSkeleton.start();
		this.commandSkeleton.start();
		Storage storageStub = Stub.create(Storage.class, this.storageSkeleton, hostname);
		Command commandStub = Stub.create(Command.class, this.commandSkeleton, hostname);

		Path[] paths = Path.list(this.root);	// List all the files on the storage server
		Path[] duplicates = naming_server.register(storageStub, commandStub, paths);	// Register these files with the naming server and get back duplicates

		for (int i = 0; i < duplicates.length; i++) {	// Delete the duplicates and prune empty directories
			File file = new File(this.root + duplicates[i].name);
			if (!file.delete()) {
				System.out.println("Cannot be deleted");
			} else {	// Pruning empty directories
				File parent = file.getParentFile();
				int len = parent.listFiles().length;
				while (len == 0) {
					File grandParent = parent.getParentFile();
					parent.delete();
					parent = grandParent;
					len = parent.listFiles().length;
				}
			}
		}
    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
    	if (storageSkeleton != null && commandSkeleton != null) {
			storageSkeleton.stop();
			commandSkeleton.stop();
		}
		stopped(null);
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
    	if (file == null) {
			throw new NullPointerException("Null path found");
		}
		File currFile = new File(this.root + file.name);
		if (!currFile.exists()) {
			throw new FileNotFoundException("File not found");
		}
		if (currFile.isDirectory()) {
			throw new FileNotFoundException("Either the file does not exists or unable to get size for the directory");
		}
		return currFile.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
    	if (file == null) {
			throw new NullPointerException("Null path found");
		}
		File currFile = new File(this.root + file.name);
		if (!currFile.exists() || currFile.isDirectory()) {
			throw new FileNotFoundException("File not found");
		}
		if (length < 0 || offset > currFile.length() || offset + length > currFile.length()) {
			throw new IndexOutOfBoundsException("invalid offset and/or length");
		}
		byte[] bytes = null;
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(currFile);
			bytes = new byte[length];
			fis.read(bytes, (int) offset, length);	// Read file into byte array
			
			if (bytes.length != length) {
				throw new IOException("Read could not be completed");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return bytes;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
    	if (file == null || data == null) {
            throw new NullPointerException("File or data is null");
        }
		if (offset < 0 || offset > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("Invalid offset and/or length");
        }
		File currFile = new File(this.root + file.name);
		if (!currFile.exists() || currFile.isDirectory()) {
			throw new FileNotFoundException("File does not exist or is a directory");
		}

		if (offset > currFile.length()) {	// If true, write to the file, with the given difference of file length and offset
			FileOutputStream fos = new FileOutputStream(currFile, true);	// to write at the EOF

			int len = (int) currFile.length();
			int offSet = (int) offset;
			int diff = offSet - len;

			byte[] bytes = new byte[diff];
			fos.write(bytes);
			fos.write(data);
			fos.close();
		}

		else {	// Create file out put stream and write to file starting from offset
			FileOutputStream fos = new FileOutputStream(currFile);
			fos.write(data, (int) offset, data.length);
			fos.close();
		}
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
    	if (file == null) {
			throw new NullPointerException("Null path found");
		}
		if (file.isRoot()) {	// If root, create directory from local path and given path
			File newFile = new File(this.root + file.name);
			if (newFile.mkdir()) {
				return true;
			} else
				return false;
		}
		// Create new file object and check if already exists
		File ifExists = new File(this.root + file.name);
		if (ifExists.exists()) {
			return false;
		}
		boolean isSuccess = false;
		Iterator<String> itr = file.iterator();
		String currPath = "/" + itr.next();

		while (itr.hasNext()) {	// Traverse through the path and checks if directories exist, if not then create it
			File currFile = new File(this.root + currPath);
			if (!currFile.exists()) {
				currFile.mkdir();
			} else {
				currPath = currPath + "/" + itr.next();
			}
		}
		File newFile = new File(this.root + "/" + currPath);	// Creates a new file with the currPath and the local path
		try {
			if (newFile.createNewFile()) {
				isSuccess = true;
			} else {
				isSuccess = false;
			}
		} catch (IOException e) {}
		return isSuccess;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
    	if (path == null) {
			throw new NullPointerException("Path cannot be null");
		}
		if (path.isRoot()) {	// Root cannot be deleted
			return false;
		}
		File deleteFile = new File(this.root + path.name);
		return deleteUtil(deleteFile);
    }
    
    /**
	 * Recursively deletes all files in a directory and at the end
	 * @param file
	 * @return boolean representing if the file was deleted
	 */
	public synchronized boolean deleteUtil(File file) {
		if (!file.exists()) {
			return false;
		}
		if (file.isFile()) {	
			return file.delete();
		}
		if (file.isDirectory()) { // Check if directory, if directory is empty, then delete it
			if (file.listFiles().length == 0) {
				return file.delete();
			} else {
				File[] delete_files = file.listFiles();
				for (File f : delete_files) {
					deleteUtil(f);
				}
				if (file.list().length == 0) {
					return file.delete();	// Delete the directory if no files are present 
				}
			}
		}
		return false;
	}
}
