import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class DBTable {

	private RandomAccessFile rows; // the file that stores the rows in the table 
	private long free; //head of the free list space for rows
	private int numOtherFields;
	private int otherFieldLengths[];
	private int maxFieldLength;
	private long root;
	int bucketSize; //may not need?
	public ExtHash eh;
	// add other instance variables as needed

	public DBTable(String filename, int fL[], int bsize) throws IOException {
		/*
		 * Use this constructor to create a new DBTable. 
		 * filename is the name of
		 * the file used to store the table 
		 * fL is the lengths of the otherFields
		 * fL.length indicates how many other fields are part of the row 
		 * bsize is the bucket size used by the hash index.
		 * 
		 *  A ExtHash object must be created for the key field in the table
		 *  
		 *  If a file name with filename exists, the file should be deleted
		 *  before the new file is created.
		 */
		
		File path = new File(filename);
		if (path.exists()) {
			path.delete(); 
		}
		try {
			bucketSize = bsize;
			maxFieldLength = 0;
			rows = new RandomAccessFile(path, "rw");
			numOtherFields = fL.length;
			otherFieldLengths = new int[numOtherFields];
			rows.seek(0);
			rows.writeInt(numOtherFields);
			for(int i = 0; i<fL.length; i++) {
				otherFieldLengths[i] = fL[i];
				if (otherFieldLengths[i] > maxFieldLength) {
					maxFieldLength = otherFieldLengths[i];
				}
				rows.writeInt(otherFieldLengths[i]);
			}
			free = 0; //if free == 0, free comes rows.length (end of file)
			rows.writeLong(free);
			root = rows.getFilePointer();
			//create ext hash obj
			eh = new ExtHash(filename, bsize);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
		
	}

	public DBTable(String filename) throws IOException {
		// Use this constructor to open an exisfng DBTable
		File path = new File(filename);
		try {
			rows = new RandomAccessFile(path, "rw");
			numOtherFields = rows.readInt();
			maxFieldLength = 0;
			otherFieldLengths = new int[numOtherFields];
			for(int i = 0; i < numOtherFields; i++) {
				otherFieldLengths[i] = rows.readInt();
				if (otherFieldLengths[i] > maxFieldLength) {
					maxFieldLength = otherFieldLengths[i];
				}
			}
			bucketSize = 0; //can read in bucket size at 0th byte of file on first read
			free = rows.readLong();
			root = rows.getFilePointer();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		//create ext hash object
		eh = new ExtHash(filename);
	}

	public boolean insert(int key, char fields[][]) throws IOException {
		// PRE: the length of each row is fields matches the expected length /*
		/*
		 * If a row with the key is not in the table, the row is added and the
		 * method returns true otherwise the row is not added and the method
		 * returns false.
		 */
		long dbTableAddr = getFree();
		Row newRow = new Row(key, fields);
		
		boolean insertBool = eh.insert(key, dbTableAddr);
		if (insertBool == true) {//key was inserted into the table
			newRow.writeRow(dbTableAddr);
		} else {
			addFree(dbTableAddr); //put free back
		}
		return insertBool;
	}

	private class Row {
		private int keyField;
		private char otherFields[][];
		int numBytes = 0;
		/*
		 * Each row consists of unique key and one or more character array
		 * fields. Each character array field is a fixed length field (for
		 * example 10 characters). Each field can have a different length.
		 * Fields are padded with null characters so a field with a length of of
		 * x characters always uses space for x characters.
		 */
		// Constructors and other Row methods
		
		private Row(int key, char[][] fieldInfo) throws IOException {
			keyField = key;
			otherFields = new char[numOtherFields][maxFieldLength];
			for(int i = 0; i < numOtherFields; i++) { //rows
				for(int j = 0; j < maxFieldLength; j++ ){ //columns
					if (i >= fieldInfo.length || j >= fieldInfo[i].length) {
						otherFields[i][j] ='\0'; //pad with null characteres if past fieldInfo's length
					} else if ( i < fieldInfo.length && j < fieldInfo[i].length){
						otherFields[i][j] = fieldInfo[i][j]; //else read in the characters
					}
				}
			}
			numBytes = 4 + fieldInfo.length + fieldInfo[0].length;
			while(numBytes < 8) { //for making it at least 8 bytes long
				numBytes++;
			}
		}
		
		private Row (long addr) throws IOException {//constructor for a row in the DBTable at address addr -- read in data
			if(addr >= rows.length()) {
				return;
			}
			rows.seek(addr);
			keyField = rows.readInt();
			otherFields = new char[numOtherFields][maxFieldLength];
			for(int i = 0; i < numOtherFields; i++) {
				for(int j = 0; j < maxFieldLength; j++){
					char theChar = rows.readChar();
					otherFields[i][j] = theChar;
				}
			}
		}
		
		private void writeRow(long addr) throws IOException{
			rows.seek(addr);
			rows.writeInt(keyField);
			for(int i = 0; i < numOtherFields; i++) {
				for(int j = 0; j < maxFieldLength; j++) {
					char theChar = otherFields[i][j];
					rows.writeChar(theChar);
				
				}
			}//end for
			if(otherFields.length > 0) {
				numBytes = 4 + otherFields.length + otherFields[0].length;
				while(numBytes < 8) {
					rows.writeChar('\0');
					numBytes++;
				}
			}	
		}
		
		private void printRow() {
			String fields = "";
			for(int i = 0; i < numOtherFields; i++) { //rows
				for(int j = 0; j < maxFieldLength; j++ ){ //columns
					if(otherFields[i][j] != '\0') {
						fields+= otherFields[i][j];
					}
				}
				fields += " ";
			}	
			System.out.println("keyField: " + keyField + " contents: " + fields);
		} //end method
	}
	
	

	public boolean remove(int key) throws IOException {
		/*
		 * If a row with the key is in the table it is removed and true is
		 * returned otherwise false is returned.
		 */
		boolean inTable = false;
		long dbTableAddr = eh.search(key);
		if(dbTableAddr == 0) {
			inTable = false;
		} else {
			inTable = true;
			eh.remove(key);
			free = addFree(dbTableAddr);
		}
		
		return inTable;
	}

	public LinkedList<String> search(int key) throws IOException {
		/*
		 * If a row with the key is found in the table return a list of the
		 * other fields in the row. The string values in the list should not
		 * include the null characters. If a row with the key is not found
		 * return an empty list The method must use the equality search
		 */
		String fieldInfo = "";
		Row searchRow = null;
		LinkedList<String> list = new LinkedList<>();
		long dbTableAddr = eh.search(key);
				//bt.search(key);
		if (dbTableAddr != 0) { //found something
			list = new LinkedList<String>();
			rows.seek(dbTableAddr);
			searchRow = new Row(dbTableAddr);
			if(searchRow.otherFields == null) {
				return list;
			}
			int tempKey = searchRow.keyField;
			fieldInfo = tempKey + "";
			list.add(fieldInfo);
			fieldInfo ="";
			for(int i = 0; i < searchRow.otherFields.length; i++) {
				for(int j = 0; j < searchRow.otherFields[i].length; j++) {
					char theChar = searchRow.otherFields[i][j];
					if(theChar != '\0') {
						fieldInfo += theChar;
					}
				}
				list.add(fieldInfo);
				fieldInfo = "";	
			}
		}
		return list;
	}

	/**
	 *  if there's no free space, returns the end of file.
	 *  if there's something the free list, returns the next address in the list and updates free list
	 * @return the address of the next free space to be used
	 * @throws IOException
	 */
	private long getFree() throws IOException { //may need to update? corner cases
		//BTreeNode temp;
		long tempAddr;
		if (free == 0) { //nothing in the free list
			return rows.length();
		} else { //something in the free list -- TEST
			rows.seek(free);
			tempAddr = free;
			if(rows.getFilePointer() > rows.length() - 8) {
				free = 0; //updating free 
			} else {
				free = rows.readLong();
			}
			

			return tempAddr;
		}
	}
	
	/**
	 * adds a new address to the front of the free list from the node being removed
	 * @param addr address of the node being added to the free list
	 * @return the new front of the free list
	 * @throws IOException
	 */
	private long addFree(long addr) throws IOException {
		long tempAddr = free; //save old free in tempArr
		free = addr; // free is now the new address
		Row newRow = new Row(addr);
		newRow.keyField = 0;
		char[][] tempFields = new char[numOtherFields][maxFieldLength];
		for(int i = 0; i < tempFields.length; i++) {
			for(int j = 0; j < tempFields[i].length; j++) {
				tempFields[i][j] = '\0';
			}
		}
		newRow.otherFields = tempFields;
		newRow.writeRow(addr); //making sure its all 0's in file

		rows.seek(free);
		if(free == tempAddr) { //new free == the old free so put it's next addr as 0
			rows.writeLong(0); //make the next free address 0
		} else {
			rows.writeLong(tempAddr); //make the old row's next bytes be the last free space address
		}
		
		return addr;
	}
	
	public void print() throws IOException {
		// Print the rows to standard output in ascending order (based on the
		// keys) //One row per line
		String fieldInfo = "";
		Row curRow = null;
		Row listRow = null;
		LinkedList<Row> list = new LinkedList<Row>();
		long dbTableAddr = root;
		int numAdded = 0;
		while (dbTableAddr < rows.length()) { //found something
			curRow = new Row(dbTableAddr);
			if(list.isEmpty()) {
				curRow.printRow();
				list.add(curRow);
			} else {
				for(int i = 0; i < numAdded; i++) {
					listRow = list.get(i);
					if(listRow.keyField < curRow.keyField) {
						list.add(i-1,curRow);
					} else if (i == list.size() -1) {
						list.addLast(curRow);
					}
				}
			}
			numAdded++;
			if(rows.getFilePointer() == dbTableAddr) {
				break;
			}
			dbTableAddr = rows.getFilePointer();
		} 
		for(int i = 0; i < list.size(); i ++) {
			curRow = list.get(i);
			curRow.printRow();
		}
	}

	public void close() throws IOException {
		// close the DBTable. The table should not be used aler it is closed
		if(free == 0) {
			free = getFree();
		}
		//update free then close
		rows.seek(0);
		rows.writeInt(numOtherFields);
		rows.seek((numOtherFields + 1)*4); //if 2 other fields, seeks to 12th byte
		//System.out.println("ON CLOSE OF FILE THE FREE SPACE IS " + free);
		rows.writeLong(free);
		eh.close();
		rows.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[] fieldLengthss = new int[2];
		fieldLengthss[0] = 10;
		fieldLengthss[1] = 10;
		LinkedList<LinkedList<String>> rangeList;
		LinkedList<String> stringList;
		char[][] arr = {{'A','n','t','o','n'}, {'C','h','e','c','k','h','o','v'}};
		char[][] arr2 = {{'B','a','x','t','e', 'r'}, {'C','h','e','c','k','h','o','v'}};
		try {
			DBTable table = new DBTable("DBTableFile", fieldLengthss, 2);
			table.insert(50, arr);
			table.insert(45, arr2);
			char[][] arr3 = {{'A','a','x','t','e', 'r'}, {'C','h','e','c','k','h','o','v'}};
			char[][] arr4 = {{'C','a','x','t','e', 'r'}, {'C','h','e','c','k','h','o','v'}};
			char[][] arr5 = {{'D','a','x','t','e', 'r'}, {'C','h','e','c','k','h','o','v'}};
			table.insert(10, arr3);
			table.insert(20, arr4);
			table.insert(30, arr5);
			table.print();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}


/*
 * 			LinkedList<String> list = table.search(50);
			for(int i = 0; i < list.size(); i++) {
				System.out.println("list at " + i + " is " + list.get(i));
			}
			list = table.search(45);
			for(int i = 0; i < list.size(); i++) {
				System.out.println("list at " + i + " is " + list.get(i));
			}
			*/
