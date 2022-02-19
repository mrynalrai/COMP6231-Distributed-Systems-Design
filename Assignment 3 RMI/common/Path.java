package common;

import java.io.*;
import java.util.*;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Serializable
{
	public String name = null;
	
    /** Creates a new path which represents the root directory. */
    public Path()
    {
    	this.name = "/";
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
    	if (component.contains("/") || component.contains(":") || component.length() == 0) {
			throw new IllegalArgumentException("Incorrect Argument");
		}

		// if path is root, concatenate root and component and return
		if (path.isRoot()) {
			this.name = path.toString() + component;
		}
		else
			this.name = path.toString() + "/" + component;	// "/data"
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
    	if (path.length() == 0) {
			throw new IllegalArgumentException("Incorrect Argument");
		}
		if (!path.substring(0, 1).equals("/") || path.contains(":")) {
			throw new IllegalArgumentException("Incorrect Argument");
		}
		String currPath = path.replaceAll("(.)\\1{1,}", "$1");	// Removing all empty components

		if ((currPath.length() != 1) && (currPath.lastIndexOf("/") == currPath.length() - 1)) {
			this.name = currPath.substring(0, currPath.length() - 1);
		} else
			this.name = currPath;
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
    	return new PathIterator();
    }
    
 	/**
 	 * Implements path iterator
 	 *
 	 */
 	public class PathIterator implements Iterator<String> {
 		int pos = 0;
 		String temp = name.substring(1, name.length());	// Removing first "/"
 		String[] pathComponents = temp.split("/");	// Transforms path to a list of components

 		@Override
 		public boolean hasNext() {
 			if (pos <= pathComponents.length - 1) {
 				return true;
 			} else
 				return false;
 		}

 		@Override
 		public String next() {
 			if (this.hasNext()) {
 				return pathComponents[pos++];
 			} else {
 				throw new NoSuchElementException();
 			}
 		}

 		@Override
 		public void remove() {
 			throw new UnsupportedOperationException();
 		}
 	}

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
    	if (!directory.exists()) {
			throw new FileNotFoundException("Directory does not exist");
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException("Not a directory");
		}
		ArrayList<Path> paths = new ArrayList<>();		// Create new array list to store list of paths
		Path currentDir = new Path();	// Set current path to root
		recursiveList(directory, currentDir, paths);	// Lists all the files present in the directory
		Path[] pathArr = new Path[paths.size()];
		for (int i = 0; i < paths.size(); i++) {
			pathArr[i] = paths.get(i);
		}

		return pathArr;

    }
    

	/*
	 * recursiveList - recursively lists all files starting from directory and saves
	 * it in p currdir argument allows to maintain a full path to each file
	 */

	/**
	 * Lists all files starting from the given directory
	 * @param directory
	 * @param currentDir
	 * @param paths contains full path to all the files in the directory
	 */
	public static void recursiveList(File directory, Path currentDir, ArrayList<Path> paths) {	
		File[] files = directory.listFiles();	// Lists all the files from the directory
		String[] fileNames = directory.list();	// Lists all the file names from the directory

		for (int i = 0; i < files.length; i++) {	// Iterates through each file in filelist of directory and add their path to paths list
			if (files[i].isDirectory()) {	// If the given file is a directory, enter the directory
				Path tempDir = new Path(currentDir + "/" + fileNames[i]);
				recursiveList(files[i], tempDir, paths);
			}
			if (files[i].isFile()) {	// If it is a file, add it to the list of paths
				Path newPath = new Path(currentDir, fileNames[i]);
				paths.add(newPath);
			}
		}
	}

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
    	return this.name.equals("/");
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
    	if (this.name.equals("/")) {
			throw new IllegalArgumentException("Root does not have a parent");
		}
		int slashIndex = this.name.lastIndexOf("/");
		if (slashIndex == 0) {	// If root is the parent
			return new Path();
		}
		return new Path(this.name.substring(0, slashIndex));	// Return a new path with path name from start to last index of "/" (eliminating the last component)
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
    	if (this.name.equals("/")) {
			throw new IllegalArgumentException("Root does not have parent");
		}
		int slashIndex = this.name.lastIndexOf("/");
		return this.name.substring(slashIndex + 1, this.name.length());	// Return a new substring from from last "/" 
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
    	if (this.name.contains(other.name)) {
			return true;
		} else
			return false;
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
    	return new File(root.getPath());
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
    	return this.name.equals(other.toString());
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
    	return this.name.hashCode();
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
    	return this.name;
    }
}
