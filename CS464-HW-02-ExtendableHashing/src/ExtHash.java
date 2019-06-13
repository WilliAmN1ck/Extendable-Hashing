import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ExtHash {
	RandomAccessFile buckets;
	RandomAccessFile directory;
	RandomAccessFile overflow;
	int bucketSize; //# of keys that can fit in each bucket
	int directoryBits;//indicates how many bits of the hash function are used by the directory
	//shows the # of bits to use from the hash value to figure out which # bucket to go to
	
	//vars for calculating index into RAF's
	int bucketoffset = 12;//used for calculating address to store first bucket in dir and bucket file
	int byteSizeOfBuckets; //initialized on constructor for extHash objects
	
	//vars for free space in files
	private long overflowFree, bucketsFree; 
	
	int numInsertRecurses = 0;
	
	private class Bucket {
		private int bucketBits; // the number of hash function bits used by this bucket
		private int numKeys; //the number of keys that are in the bucket
		private int keys[];
		private long tableAddrs[];
		//overflow bucket?
		private long bRoot, bFree; //may not need these
		private long overflowAddr, currAddr;
		
		//may not need this...will probably know if in an overflow bucket or not but can still use
		private boolean overflowBucket = false; //used for determining which file to do IO from (mainly used in writeBucket)
		//need to set overFlowBucket to true whenever creating it with first constructor

		//constructors and other methods
		
		//constructor for a bucket to be written to the file
		/**
		 * 
		 * @param numbKeys number of keys to insert
		 * @param keyVals values of the keys
		 * @param addrs the address of the keys in the dbtable
		 * @param overflow the address of this bucket's first overflow bucket
		 */
		private Bucket(int numbKeys, int[] keyVals, long[] addrs, long overflow) {
			bucketBits = directoryBits;
			currAddr = 0;
			numKeys = numbKeys;
			keys = new int[bucketSize];//+1 for insert splits?
			tableAddrs = new long[bucketSize];//+1 for insert splits?
			for(int i = 0; i < bucketSize; i++) {
				if ( i < keyVals.length) {
					keys[i] = keyVals[i];
					tableAddrs[i] = addrs[i];
				} else {
					keys[i] = 0;
					tableAddrs[i] = 0;
				}
				
			}
			overflowAddr = overflow;
			overflowBucket = false;
		}
		
		//constructor for a bucket that exists and is stored in the bucket or overflow file
		private Bucket(long addr, boolean isOverflow) throws IOException {
			if (!isOverflow && addr > buckets.length()) {
				return;
			} else if (isOverflow && addr > overflow.length()) {
				return;
			}
			overflowBucket = isOverflow;
			keys = new int[bucketSize]; 
			tableAddrs = new long[bucketSize]; 
			currAddr = addr; 
			if(isOverflow) {
				overflow.seek(addr);
				readBucket(overflow);
			} else {
				buckets.seek(addr);
				readBucket(buckets);
			}
		}
		/**
		 * MAY need to include a boolean for if its in the overflow file or not...
		 * @param addr
		 * @throws IOException
		 */
		private void writeBucket(long addr) throws IOException {
			
			if(overflowBucket) {
				//f = overflow;
				overflow.seek(addr);
				currAddr = addr;
				overflow.writeInt(bucketBits);
				overflow.writeInt(numKeys);
				for(int i = 0; i < bucketSize; i++) {
					overflow.writeInt(keys[i]);
				}
				for(int i = 0; i < bucketSize; i++) {
					overflow.writeLong(tableAddrs[i]);
				}
				overflow.writeLong(overflowAddr);
			} else {
				//f = buckets;
				buckets.seek(addr);
				currAddr = addr;
				buckets.writeInt(bucketBits);
				buckets.writeInt(numKeys);
				for(int i = 0; i < bucketSize; i++) {
					buckets.writeInt(keys[i]);
				}
				for(int i = 0; i < bucketSize; i++) {
					buckets.writeLong(tableAddrs[i]);
				}
				buckets.writeLong(overflowAddr);
			}
			
		}
		/**
		 * helper method for constructing a bucket already in file
		 * @param f
		 * @throws IOException
		 */
		private void readBucket(RandomAccessFile f) throws IOException {
			bucketBits = f.readInt();
			numKeys = f.readInt();
			for(int i = 0; i < bucketSize; i++) {
				keys[i] = f.readInt();
				if(keys[i] != 0) {
					//numKeys++;
				}
			}
			for(int i = 0; i < bucketSize; i++) {
				tableAddrs[i] = f.readLong();
			}
			overflowAddr = f.readLong();
		}
	}

	
	public ExtHash(String filename, int bsize) {
		//bsize is bucket size (the # of keys to fit in each bucket)
		//creatse a new hash index
		//the filename is the name of the file that contains the table rows
		//the directory file should be named filename+"dir"
		//the bucket file should be named filename+"buckets"
		//if any of the files exists they should be deleted before new ones are created
		File dirFile = new File(filename + "dir");
		File bucketFile = new File(filename + "buckets");
		File overflowFile = new File(filename + "overflow");
		try {
			if (dirFile.exists()) {
				dirFile.delete();
			}
			if(bucketFile.exists()) {
				bucketFile.delete();
			}
			if (overflowFile.exists()) {
				overflowFile.delete();
			}
			directory = new RandomAccessFile(dirFile, "rw");
			buckets = new RandomAccessFile(bucketFile, "rw");
			overflow = new RandomAccessFile(overflowFile, "rw");
			
			overflow.seek(0);
			overflowFree = 0;
			overflow.writeLong(0); // first 8 bytes in overflow file is the free space
			
			bucketSize = bsize;
			buckets.seek(0); //seek to beginning of bucket file to read in bucket size
			buckets.writeInt(bucketSize);
			bucketsFree = 0;
			buckets.writeLong(bucketsFree);// first 12 bytes of bucket file made of bucketSize and free list
			
			directoryBits = 1; // 1 initially
			directory.seek(0); //seek to beginning of dir file to read hashBits being used
			directory.writeInt(directoryBits);
			
			initDirectory();
			
			byteSizeOfBuckets = (4*2) + 8 + (bucketSize * 4) + (bucketSize * 8);
			
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @return the address of the start of the last row in the directory
	 * @throws IOException
	 */
	private long initDirectory() throws IOException{
		Bucket bucket = null;
		int[] tempKeys = new int[bucketSize];
		long[] tempAddrs = new long[bucketSize];
		tempKeys = initKeys();
		tempAddrs = initAddrs();
		bucket = new Bucket(0, tempKeys, tempAddrs, 0);
		bucket.overflowBucket = false;
		bucket.writeBucket(12); //write out first bucket to dir at 12th byte address and update its curAddr
		directory.seek(4); //seek to first row in directory (after size of dir int)
		directory.writeLong(bucket.currAddr); //write this bucket in directory 
		bucket = new Bucket(0, tempKeys, tempAddrs, 0);
		bucket.overflowBucket = false;
		bucket.writeBucket(12 + (4*2) + 8 + (bucketSize * 4) + (bucketSize * 8)); //write out next bucker and its currAddr is updated in writeBucket method
		directory.writeLong(bucket.currAddr);
		return bucket.currAddr;
	}
	public ExtHash(String filename) {
		/*
		 * open an existing hash index
		 * the associated directory file is named filename+"dir"
		 * the associated bucket file is named filename+"buckets"
		 * both files should already exist when this method is used
		 */
		try {
			File bucketFile = new File(filename + "buckets");
			File dirFile = new File(filename + "dir");
			File overFlowFile = new File(filename +"overflow");

			buckets = new RandomAccessFile(bucketFile, "rw");
			directory = new RandomAccessFile(dirFile,"rw");
			overflow = new RandomAccessFile(overFlowFile,"rw");
			
			directory.seek(0);
			directoryBits = directory.readInt();
			//dirFree = directory.readLong(); //delete
			
			buckets.seek(0);
			bucketSize = buckets.readInt();
			bucketsFree = buckets.readLong();
			
			overflow.seek(0);
			overflowFree = overflow.readLong();
			//System.out.println("DEBUG ON OPEN OF EXISTIN FILE w/ overflowFree:: " + overflowFree);
			byteSizeOfBuckets = (4*2) + 8 + (bucketSize * 4) + (bucketSize * 8); //bucketSize of 2 equals 40 bytes
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param k
	 * @return the address of the row in the dbtable
	 */
	public long search(int k) {
		/* if the key is found return the address for the row with the key
		 * otherweise return 0
		 */
		long addr = 0;
		long bucketAddr = 0;
		long overflowAddr = 0;
		int key = k;
		long tableAddr = 0;
		long dirAddr = 0; //may not need, just for debugging
		Bucket bucket = null;
		
		int hash = hash(k);
		int dirHash = hashToNBits(hash, directoryBits);
		int bucketRow = calcDirectoryBucketNum(dirHash);
		try {
			dirAddr = advanceDirectory(bucketRow);
			if(directory.getFilePointer() > directory.length()-8) {
				//System.out.println("[ERROR IN SEARCH] table address is past directory length");
				return 0;
			}
			bucketAddr = directory.readLong(); 
			
			//checking the bucket the directory links to & its overflow
			if(bucketAddr != 0 && bucketAddr < buckets.length()) { //can go to this address in the bucket
				buckets.seek(bucketAddr); //shouldn't need this...
				bucket = new Bucket(bucketAddr, false);
				tableAddr = checkKeysInBucket(k, bucket.keys, bucket.tableAddrs);
				addr = tableAddr;
				if(tableAddr != 0) { //found the key in this bucket
					return tableAddr;
				} else { //tableAddr == 0 - need to check overflow bucket(s)
					overflowAddr = bucket.overflowAddr;
					if(overflowAddr != 0 && overflowAddr < overflow.length()) { //can search the overflow buckets
						bucket = new Bucket(overflowAddr, true);
						tableAddr = checkOverflowBuckets(bucket, key); //recursively loop through overflow buckets
						//will return the ta ble address or 0 if not found -- could just return tableAddr here
						addr = tableAddr; 
					} else { //not a valid address in overflow file or no linked overflow to this bucket
						return addr; //addr = 0
					}
				}
			} else { //invalid bucket address outside of file range
				return addr; // addr = 0 here
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//will reach here from searching overflow buckets whether it is found or not
		return addr;
	}
	
	/**
	 * 
	 * @param bucket
	 * @param key
	 * @return address of key in the dbtable if it is found or 0 otherwise
	 * @throws IOException
	 */
	private long checkOverflowBuckets(Bucket bucket, int key) throws IOException {
		long dbAddr = 0;
		int[] bucketKeys = bucket.keys;
		long[] bucketTableAddrs = bucket.tableAddrs;
		for(int i = 0; i < bucketSize; i++) {
			if( key == bucketKeys[i] && bucketTableAddrs[i] != 0) {
				dbAddr = bucketTableAddrs[i];
			}
		}
		if(dbAddr == 0 && bucket.overflowAddr != 0) { //if the key has not been found, check the next overflowBucket
			dbAddr = checkOverflowBuckets(new Bucket(bucket.overflowAddr, true), key);
		} //else
		return dbAddr;
	}
	/**
	 * 
	 * @param key
	 * @param keys
	 * @param tableAddrs
	 * @return address in dbTable of the key if found, or 0 otherwise
	 */
	private long checkKeysInBucket(int key, int[] keys, long[] tableAddrs) {
		long dbAddr = 0;
		for(int i = 0; i < keys.length; i++) { //does not find keys in 2nd/last position when using bucket.numKeys
			if(key == keys[i] && tableAddrs[i] != 0) {
				dbAddr = tableAddrs[i];
			}
		}
		return dbAddr;
	}
	/**
	 * 
	 * @param dirHash
	 * @return the number of directory rows/buckets to read through to point the file pointer at the correct row to get this bucket's address for
	 */
	private int calcDirectoryBucketNum(int dirHash) {
		int temp = dirHash; //temp used to count # of addresses in directory to search through until at the correct bucket
		int bucketRow = 0;
		int numBitsChecked = 0;
		while(temp > 0) {
			if(temp % 10 == 1) {
				bucketRow += Math.pow(2, numBitsChecked);
			}
			numBitsChecked++;
			temp = temp/10;
		}
		return bucketRow;
	}
	/**
	 * 
	 * @param bucketRow the number of rows to read over until you get to the row with the correct bucket
	 * @return the address of the directory filepointer after cycling through rows in directory
	 * @throws IOException
	 */
	private long advanceDirectory(int bucketRow) throws IOException {
		int rows = 0;
		//4, 12 if had a free space but do not need
		long bucketAddr = 0;
		directory.seek(4); //seek to first row in directory file since the first 4 bytes is the hash bits without free list
		while(rows <  bucketRow) {
			bucketAddr = directory.readLong();
			rows++;
		}
		return directory.getFilePointer();
	}
	/**
	 * 
	 * @param bucket
	 * @param keyToInsert
	 * @return true if the full hashes for all the keys in the bucket and the one to insert match, false otherwise
	 */
	private boolean checkFullHashValues(Bucket bucket, int insertingKeysFullHash) {
		boolean sameHashes = true;
		int hashVals[] = new int[bucket.numKeys];
		for(int i = 0; i < bucket.numKeys; i++) { //fully hash each key in the bucket
			hashVals[i] = hash(bucket.keys[i]);
			//System.out.println("hashVals[i] = " + hashVals[i] + " keys full hash: " + insertingKeysFullHash);
		}
		for(int j = 0; j < bucket.numKeys-1; j++) { //compare each keys hash and if one doesn't equal another, sameHashes = false
			if(hashVals[j] != hashVals[j+1] || hashVals[j] != insertingKeysFullHash || hashVals[j+1] != insertingKeysFullHash) {
				sameHashes = false;
			}
		}
		return sameHashes;
	}
	/**
	 * method for handling splitting of directory and writing it to the file 
	 * increments directoryBits
	 * @return the address of the last row in directory file
	 * @throws IOException
	 */
	private long doubleDirectory() throws IOException { //should be done
		int numRows = (int)(Math.pow(2, directoryBits));
		long[] curBucketAddrs = getBucketAddrsFromDirectory();
		long lastBucketAddr = advanceDirectory(numRows); //do not want to seek to directory.length() b/c 
		//file may be bigger than directory if there were deletions
		directory.seek(lastBucketAddr);
		int newRowsMade = 0;
		while(newRowsMade < numRows) { //write out addresses to the old buckets for each new directory row
			directory.writeLong(curBucketAddrs[newRowsMade]);
			newRowsMade++;
		}
		directoryBits++;
		return directory.getFilePointer()-8;
	}
	/**
	 * 
	 * @return the addresses to all buckets in the current directory
	 * @throws IOException
	 */
	private long[] getBucketAddrsFromDirectory() throws IOException {
		long[] bucketAddrs = new long[(int) Math.pow(2, directoryBits)];
		directory.seek(4); //seek to first row in directory
		for(int i = 0; i < bucketAddrs.length; i++) {
			bucketAddrs[i] = directory.readLong();
		}
		return bucketAddrs;
	}
	/**
	 * 
	 * @param key the key to rehash or could input it's hash value if needed
	 * @param bucket the bucket to be split
	 * @return the address of the newly created bucket in the bucket file
	 * @throws IOException 
	 */
	//need to go thru each existing bucket and rehash every single key and place in correct row in directory and correct bucket in 
	//bucket file
	private long splitBucket(int key, Bucket bucket) throws IOException { //splitting this bucket should point the directory at the correct row to the new split bucket
		long newBucketAddr = 0;
		int numKeysOriginal = 0;
		int numKeysNew = 0;
		Bucket newBucket = null, overflowBucket = null;
		int originalBucketHash = Integer.MAX_VALUE; //used for calculating the directory row
		int newBucketHash = 0;
		int[] originalBucketKeys = initKeys();
		int[] newBucketKeys = initKeys();
		long[] originalBucketAddrs = initAddrs();
		long[] newBucketAddrs = initAddrs();
		int[] keyHashes = initKeys();
		
		long[] tempAddrs = initAddrs(); //really do not need...
		int newBucketRow = 0;
		//System.out.println("split bucket at addr : " + bucket.currAddr + " for key : " + key + " with bucketBits: " + bucket.bucketBits + " and dirBits " + directoryBits);
		int hashNBitsOfKey = hashToNBits(hash(key),directoryBits);
		//int otherHash = 0;
		
		long overflowAddr = bucket.overflowAddr;
		if(overflowAddr != 0 ) { //for which bucket overflowAddr may need to follow to
			overflowBucket = new Bucket(overflowAddr, true);
		}
		
		for(int i = 0; i < bucketSize; i++) { //hash each key to new directory bits
			keyHashes[i] = hashToNBits(hash(bucket.keys[i]), directoryBits);
			tempAddrs[i] = bucket.tableAddrs[i];
			if(keyHashes[i] < originalBucketHash) {
				originalBucketHash = keyHashes[i];
			}
			if(keyHashes[i] > newBucketHash) {
				newBucketHash = keyHashes[i];
			}
		}
		if(newBucketHash == originalBucketHash) { //new key hashes to same row
			if(newBucketHash != hashNBitsOfKey) { //above loops didnt take correct Hash...
				newBucketHash = hashNBitsOfKey;
			} else { //all keys hash to same N bits but full hashes differ so directory needs to be doubled for split to work
				doubleDirectory();
				return splitBucket(key, bucket);
			}
		}
		for(int i = 0; i < bucketSize; i++) { //separate keys and their addresses
			if(keyHashes[i] == originalBucketHash) {
				originalBucketKeys[numKeysOriginal] = bucket.keys[i];
				originalBucketAddrs[numKeysOriginal] = tempAddrs[i]; 
				numKeysOriginal++;
			} else if (keyHashes[i] == newBucketHash) {
				newBucketKeys[numKeysNew] = bucket.keys[i];
				newBucketAddrs[numKeysNew] = tempAddrs[i];
				numKeysNew++;
			}
		}
		newBucket = new Bucket(numKeysNew, newBucketKeys, newBucketAddrs, 0);
		bucket.keys = originalBucketKeys;
		
		if(overflowAddr != 0) { //if there is overflow, check which bucket should get the overflow address
			if (checkFullHashValues(bucket, hash(overflowBucket.keys[0]))) {
				bucket.overflowAddr = overflowAddr;
			} else {
				newBucket.overflowAddr = overflowAddr;
			}
		}
		
		bucket.tableAddrs = originalBucketAddrs;
		bucket.numKeys = numKeysOriginal;
		bucket.bucketBits = directoryBits;
		bucket.writeBucket(bucket.currAddr); //write out the original bucket to its old address
		int originalBucketRow = calcDirectoryBucketNum(originalBucketHash); //added 4/13/19
		advanceDirectory(originalBucketRow);//added 4/13/19
		directory.writeLong(bucket.currAddr); //added 4/13/19
		
		newBucketRow = calcDirectoryBucketNum(newBucketHash);
		newBucketAddr = getBucketFree();
		newBucket.writeBucket(newBucketAddr); //write out newly split bucket to next free address in bucket file
		//System.out.println("original bucket row is : " + originalBucketRow + " new bucket row is: " + newBucketRow);
		directory.seek(advanceDirectory(newBucketRow)); //seek directory to the row of the new bucket
		directory.writeLong(newBucketAddr); //write out the new Bucket's address at the row in the directory

		return newBucketAddr;
	}

	/**
	 * 
	 * @param buckToKeep
	 * @param buckToDelete
	 * @return the new bucket from combining buckets
	 * @throws IOException
	 */
	private Bucket combineBuckets(Bucket buckToKeep, Bucket buckToDelete) throws IOException { //point directory position of bucket to delete to the bucket to keep
		int[] combinedKeys = initKeys();
		long[] combinedAddrs = initAddrs();
		int combinedCount = 0;
		for(int i = 0; i < bucketSize && combinedCount < bucketSize; i++) {
			if(buckToKeep.tableAddrs[i] != 0) {
				combinedKeys[combinedCount] = buckToKeep.keys[i];
				combinedAddrs[combinedCount] = buckToKeep.tableAddrs[i];
				combinedCount++;
			}
			if(buckToDelete.tableAddrs[i] != 0) {
				combinedKeys[combinedCount] = buckToDelete.keys[i];
				combinedAddrs[combinedCount] = buckToDelete.tableAddrs[i];
			
				combinedCount++;
			}
			buckToDelete.keys[i] = 0;
			buckToDelete.tableAddrs[i] = 0;
		}
		buckToDelete.numKeys = 0;
		buckToKeep.keys = combinedKeys;
		buckToKeep.tableAddrs = combinedAddrs;
		buckToKeep.numKeys = combinedCount;
		buckToKeep.bucketBits--;
		long lastOverflowBucketAddr = 0; //last overflowBucketAddr for bucket to KEep
		//link this to the first buck to delete overflowaddr
		if(buckToKeep.overflowAddr != 0 && buckToDelete.overflowAddr != 0) { //both buckets have overflow..
			lastOverflowBucketAddr = buckToKeep.overflowAddr;
			Bucket overflowBucket = new Bucket(lastOverflowBucketAddr, true);
			while(overflowBucket.overflowAddr != 0) { //walk to last overflowAddr in bucket to keep
				overflow.seek(lastOverflowBucketAddr + (4 + 4 + (4*bucketSize) + (8*bucketSize)));
				lastOverflowBucketAddr = overflow.readLong();
				overflowBucket = new Bucket(lastOverflowBucketAddr, true);
			}
			//at last overflowBucket in buckToKeep list and need to link
			overflowBucket.overflowAddr = buckToDelete.overflowAddr;
			overflowBucket.writeBucket(overflowBucket.currAddr);
		} else if (buckToDelete.overflowAddr != 0) {
			buckToKeep.overflowAddr = buckToDelete.overflowAddr;
		}
		//write combined buckets to file
		buckToKeep.writeBucket(buckToKeep.currAddr);
		buckToDelete.overflowAddr = 0;
		buckToDelete.writeBucket(buckToDelete.currAddr);
		addFreeBucketsOrOverflow(buckToDelete.currAddr, true);//add removed bucket addr to free list
		return buckToKeep;
	}
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	private long shrinkDirectory() throws IOException {
		directoryBits--;
		long lastBucketAddr = 0;
		int newDirSize = (int) Math.pow(2, directoryBits);
		long[] bucketAddrsToWriteToDir = new long[newDirSize];
		int numAddrsToWriteToDir = 0;
		directory.seek(4);//seek to 1st row in directory
		long bucketAddr= 0;
		while(numAddrsToWriteToDir < newDirSize) { //loop through the new smaller directory size and copy bucket addrs after split
			boolean found = false;
			bucketAddr = directory.readLong();
				bucketAddrsToWriteToDir[numAddrsToWriteToDir] = bucketAddr;
				numAddrsToWriteToDir++;
		}
		directory.seek(4); //start of dir file after dir size int
		for(int i = 0; i< newDirSize*2; i++) { //write each address in bucketAddrsToWriteToDir
			if(i < newDirSize) {
				directory.writeLong(bucketAddrsToWriteToDir[i]);
			} else {
				directory.writeLong(0); //0 out old directory addresses
			}
			
		}
		return lastBucketAddr;
	}
	/**
	 * 
	 * @return true if every pair of buckets points to same address, false otherwise
	 * @throws IOException
	 */
	private boolean canCollapse() throws IOException {
		//System.out.println(" dir bits = " + directoryBits);
		boolean canCollapse = true;
		int dirSize = (int) Math.pow(2, directoryBits);
		int halfDirSize = dirSize/2;
		int curBucketRow = 0, buddyBucketRow = 0;
		long curBuckAddr = 0, buddyBuckAddr = 0;
		long dirAddr =0;
		for(int i = 0; i < halfDirSize; i++) { //read through half of the directory rows
			curBucketRow = i;
			buddyBucketRow = i + halfDirSize;
			dirAddr = advanceDirectory(curBucketRow);
			curBuckAddr = directory.readLong();
			dirAddr = advanceDirectory(buddyBucketRow);
			buddyBuckAddr = directory.readLong();
			if(buddyBuckAddr != curBuckAddr) {
				canCollapse = false;
				return false;
			}
		}
		return canCollapse;
	}
	/**
	 * 
	 * @param bucketAddr the address of the bucket to search for in the directory
	 * @return
	 * @throws IOException
	 */
	private boolean canDouble(long bucketAddr) throws IOException{
		int numAddrsInDir = 0;
		long dirAddr = 0;
		directory.seek(4);
		dirAddr = directory.readLong();
		while(dirAddr != 0) {
			if(dirAddr == bucketAddr) {
				numAddrsInDir++;
			}
			if(directory.getFilePointer() <= directory.length() -8) {
				dirAddr = directory.readLong();
			} else {
				dirAddr = 0;
			}
		}
		return numAddrsInDir == 1; //returns true if the address of the bucket only appears once in directory
	}
	/**
	 * 
	 * @param key
	 * @return the address of the key in the dbtable
	 */
	public long remove(int key) {
		/*
		 * if the key is int he hash index, remove the key and return the row
		 * return 0 if the key is not found in the hash index
		 */
		int fullHash = 0;
		int dirBitsHash = 0;
		int bucketRowInDir = 0, buddyBucketRowInDir = 0;
		long dirAddr = 0, bucketAddr = 0,buddyBucketAddr = 0;
		Bucket bucket = null, buddyBucket = null;
		int keyPos = 0;
		long overflowAddr = 0;
		long dbTableAddr = search(key);
		if (dbTableAddr != 0) { //key is found
			fullHash = hash(key);
			dirBitsHash = hashToNBits(fullHash, directoryBits); //take the full hash and hash it to the directory row bits
			bucketRowInDir = calcDirectoryBucketNum(dirBitsHash); //calculate which row in the directory to seek to
			//found this bucket's row, now need to find buddy bucket's row in case of combine
			buddyBucketRowInDir = getBuddyBucket(bucketRowInDir);
			try {
				dirAddr = advanceDirectory(buddyBucketRowInDir);
				buddyBucketAddr = directory.readLong(); //address to use to create buddyBucket 
				dirAddr = advanceDirectory(bucketRowInDir); //set directory filePointer to the row with the bucket
				bucketAddr = directory.readLong();
				
				if(bucketAddr != 0 && bucketAddr < buckets.length()) { //valid bucket addr to remove from
					bucket = new Bucket(bucketAddr, false);
					//check if bucket will need to be combined or not
				} else { //not valid bucketAddr
					System.out.println("[ERROR]: BucketAdrr is not valid in remove! it is: " +  bucketAddr);
					return 0;
				}
				if(buddyBucketAddr != 0  && buddyBucketAddr < buckets.length()) {
					buddyBucket = new Bucket(buddyBucketAddr, false);
				} else {
					System.out.println("[ERROR]: buddyBucketAdrr is not valid in remove! it is: " +  buddyBucketAddr);
					return 0;
				}
				keyPos = getKeyPosition(bucket, key);
				if(keyPos == -1) { //did not find key in this bucket & need to search overflow file...
					overflowAddr = bucket.overflowAddr;
					bucket.overflowAddr = searchAndRemoveFromOverflow(overflowAddr, key);
					if(bucket.overflowAddr == 0) {
						//System.out.println("[DEBUG] either did not remove key or removed key from last bucket in overflow file");
					}
					bucket.writeBucket(bucket.currAddr);
				} else { //key in bucketFile...
					//delete key and it's addr
					bucket.keys[keyPos] = 0;
					bucket.numKeys--;
					dbTableAddr = bucket.tableAddrs[keyPos]; //store key's addr to be returned at end
					bucket.tableAddrs[keyPos] = 0;
					bucket.writeBucket(bucket.currAddr);//write out this bucket before combining..
					//check if bucket can be combined and directory can be shrunk after this key was removed		
					while(directoryBits > 1 &&((bucket.numKeys) + buddyBucket.numKeys <= bucketSize) && bucket.currAddr != buddyBucket.currAddr) { //keep combining buddybucket into the bucket if can
						bucket = combineBuckets(bucket, buddyBucket); //both buckets are written out to file
						directory.seek(advanceDirectory(buddyBucketRowInDir)); //seek to deleted bucket's row
						directory.writeLong(bucket.currAddr); //update the buddy bucket's directory to point to the combined bucket addr
						//check if can shrink directory...
						if (canCollapse()) { //check to see if can collapse the directory
							shrinkDirectory();
							//try combining same bucket until can't anymore
							dirBitsHash = hashToNBits(fullHash, directoryBits); //take the full hash and hash it to the directory row bits
							bucketRowInDir = calcDirectoryBucketNum(dirBitsHash); //calculate which row in the directory to seek to for kept bucket
							//found this bucket's row, now need to find buddy bucket's row to try to combine
							buddyBucketRowInDir = getBuddyBucket(bucketRowInDir);
							advanceDirectory(buddyBucketRowInDir);
							buddyBucketAddr = directory.readLong(); //address to use to create buddyBucket 
							buddyBucket = new Bucket(buddyBucketAddr, false);
						} else {
							//System.out.println("[REMOVE DEBUG] BREAKING");
							return dbTableAddr;
						}
					}
				}	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else { //key not in table
			//System.out.println("[DEBUG] key : " + key + " was not found in table during remove!");
		}
		return dbTableAddr;
	}
	/**
	 * 
	 * @param overflowBucketAddr
	 * @param key
	 * @return the address of the next overflow bucket -- keep linking together if one is deleted
	 * @throws IOException
	 */
	private long searchAndRemoveFromOverflow(long overflowBucketAddr, int key) throws IOException {
		long overflowAddr = overflowBucketAddr;
		if(overflowBucketAddr == 0 || overflowBucketAddr > overflow.length()) {
			return 0; //failed/could not find
		}
		Bucket oBucket = new Bucket(overflowBucketAddr, true);
		boolean foundKey = false;
		for(int i = 0; i < bucketSize; i++) {
			if(oBucket.keys[i] == key && oBucket.tableAddrs[i] != 0) { //found the key in this bucket
				oBucket.keys[i] = 0;
				oBucket.tableAddrs[i] = 0;
				oBucket.numKeys--;
				if(oBucket.numKeys == 0) {
					overflowAddr = oBucket.overflowAddr; //return the next bucket's overflow addr or 0 if at end
					oBucket.writeBucket(overflowBucketAddr); //write out new bucket to file
					addFreeBucketsOrOverflow(overflowBucketAddr, false);
				} else { //just write out bucket and return this bucket's address since not deleting
					oBucket.writeBucket(overflowBucketAddr);
				}
				foundKey = true;
			}
		}
		if(!foundKey) {
			oBucket.overflowAddr = searchAndRemoveFromOverflow(oBucket.overflowAddr, key);
			oBucket.writeBucket(oBucket.currAddr);
		}
		
		return overflowAddr;
	}
	
	/**
	 *  used in remove
	 * @param bucket
	 * @param keyToRemove
	 * @return the position of the key in the bucket
	 */
	private int getKeyPosition(Bucket bucket, int keyToRemove) {
		int position = -1;
		for(int i = 0; i < bucketSize; i++) {
			if(bucket.keys[i] == keyToRemove && bucket.tableAddrs[i] != 0) {
				position = i;
			}
		}
		return position;
	}
	/**
	 * used in remove
	 * @param bucketRowInDir
	 * @return position of the buddy bucket row in the directory
	 */
	private int getBuddyBucket(int bucketRowInDir) {
		int numDirSpots = (int) Math.pow(2, directoryBits);
		int halfDirSize = numDirSpots/2;
		int buddyBucketRow = 0;
		
		if(bucketRowInDir > (halfDirSize-1)) { //buddyBucket will be in the bottom half of directory
			buddyBucketRow = bucketRowInDir - halfDirSize;
		} else { //buddy bucket row will be in upper half of directory
			buddyBucketRow = bucketRowInDir + halfDirSize;
		}
		return buddyBucketRow;
	}
	
	/**
	 * 	if the keys full hash code's are the same and the bucket is full, do not split -- add to overflow
	 * 		keys hashed N bits match but full keys don't match and bucket is full -- split bucket 
	 * 			need to check if directory needs to be split
	 * @param key
	 * @param addr
	 * @return true if key was inserted, false if key is a duplicate
		 * if the key is not a duplicate, add key to the hash index
		 * addr is the address of the row that contains the key
		 * return true if the key is added
		 * return false if the key is a duplicate
	 */
	public boolean insert (int key, long addr) {
		boolean inserted = false;
		int fullHash = 0;
		int dirBitsHash = 0;
		Bucket bucket = null;
		int bucketRowInDir = 0;
		long dirAddr = 0;
		//vars for when splitting buckets
		long newBucketAddr = 0;
		long bucketAddr = 0;
		
		long tableAddr = search(key);
		
		if(tableAddr == 0) { //not in the table
			fullHash = hash(key);
			dirBitsHash = hashToNBits(fullHash, directoryBits);
			bucketRowInDir = calcDirectoryBucketNum(dirBitsHash);
			try {
				dirAddr = advanceDirectory(bucketRowInDir);
				bucketAddr = directory.readLong();
				if(bucketAddr != 0 && bucketAddr < buckets.length()) { //valid bucket addr to insert into
					bucket = new Bucket(bucketAddr, false);
					//check if bucket has room for the key
					if(bucket.numKeys < bucketSize) {
						//if it has room, just add it and update file and other vars..
						bucket = updateBucket(bucket, key, addr);
						//System.out.println("[ADDING A KEY HERE]: adding key : " + key + " to bucketADDR" + bucket.currAddr);
						bucket.writeBucket(bucketAddr);
						numInsertRecurses=0;
					} else { //TODO
						//else bucket is full
						//check if each key in bucket hashes to the same thing == overflow
						if (checkFullHashValues(bucket,fullHash)) { //add to overflow and update this bucket's overflowAddr or the last one in the list
							bucket.overflowAddr = addToOverflow(key, addr, bucket.overflowAddr);
							//should be done..? need to TEST
							//System.out.println("IN REMOVE AND ADDING KEY " + key + " to overflow");
							bucket.writeBucket(bucket.currAddr);
						} else { //need to split bucket
							if (canDouble(bucketAddr)) { //need to double directory size
								//System.out.println("DOUBLING DIRECTORY SIZEDOUBLING DIRECTORY SIZEDOUBLING DIRECTORY SIZEDOUBLING DIRECTORY SIZEDOUBLING DIRECTORY SIZE");
								long addrOfLastRowInDir = doubleDirectory();
								//directory bits and addresses allocated in double directory
							}
							newBucketAddr = splitBucket(key, bucket);
							/*
							if(numInsertRecurses > 3) { //stop stack overflow and just add to this bucket
								numInsertRecurses = 0;
								bucket.overflowAddr = addToOverflow(key, addr, bucket.overflowAddr);
							} else {
								numInsertRecurses++;
								insert(key, addr);
							}
							*/
							insert(key,addr);
						}
					}	
				} else if (bucketAddr == 0 && directoryBits == 1) { //directory not initialized to a bucket yet? need to create an empty bucket and insert
					System.out.println("ERROR] SHOULD NOT HAVE REACHED HERE IN INSERT! BUCKET ADDR IS 0 AND PAST END OF BUCKET FILE " + bucketAddr);
					insert(key, addr);
				} else {
					System.out.println("[ERROR] key " + key + " not in table and did not find a clause for this bucketAddr: " + bucketAddr);
					System.out.println("[DEBUG] buckets.legnth is : " + buckets.length() + " dirlenth is: " + directory.length());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			inserted = true; //update inserted
		}  else {
			//System.out.println("[DEBUG] key " + key + " is a duplicate and was not inserted!");
		}
		return inserted;
	}
	/**
	 * 
	 * @param bucket
	 * @param key
	 * @param tableAddr
	 * @return the updated Bucket with newly inserted key to be written to the bucket file
	 */
	private Bucket updateBucket(Bucket bucket, int key, long tableAddr) {
		int numKeys = bucket.numKeys;
		boolean inserted = false;
		int count = 0;
		while(count < bucketSize && !inserted) {
			if(bucket.tableAddrs[count] == 0) {
				bucket.numKeys++;
				bucket.keys[count] = key;
				bucket.tableAddrs[count] = tableAddr;
				inserted = true;
			}
			count++;
		}
		return bucket;
	}
	/**
	 * recursive method to add buckets to overflow file
	 * @param key
	 * @param tableAddr
	 * @param overflowAddr
	 * @return the address of the overflow bucket with the key to be added
	 * @throws IOException
	 */
	private long addToOverflow(int key, long tableAddr, long overflowAddr) throws IOException {
		Bucket bucket = null;
		long bucketAddr = 0;
		int[] tempKeys = null;
		long[] tempAddrs = null;
		if (overflowAddr == 0) {
			//grab a new free space from overflow file
			overflowAddr = getOverflowFree();
			//create a new bucket in overflow file and return the overflowAddr
			tempKeys = initKeys();
			tempAddrs = initAddrs();
			tempKeys[0] = key;
			tempAddrs[0] = tableAddr;
			bucket = new Bucket(1,tempKeys, tempAddrs, 0);
			bucket.overflowBucket = true;
			bucket.writeBucket(overflowAddr);
		} else {//inserting into a bucket already in the file
			bucket = new Bucket(overflowAddr, true);
			if(bucket.numKeys < bucketSize) { //can fit the key here
				bucket.keys[bucket.numKeys] = key;
				bucket.tableAddrs[bucket.numKeys] = tableAddr;
				bucket.numKeys++;
				bucket.writeBucket(overflowAddr);
			} else { //check if theres a next overflow bucket linked
				bucket.overflowAddr = addToOverflow(key, tableAddr, bucket.overflowAddr);
				bucket.writeBucket(bucket.currAddr);
			}
		}
		return overflowAddr;
	}
	/**
	 * 
	 * @param key
	 * @return the full hashed value of the key as an integer representing it's binary string form
	 */
	public int hash (int key) {
		//return the hash value
		if( key == 0) { //for handling keys that are 0 in extHash
			key = 1;
		} else if(key < 0){
			key = key*-3;
		}
		String hash = Integer.toBinaryString(key);

		int hashVal = Integer.parseInt(hash);
		
		int bitsToKeep = (int) Math.pow(2, directoryBits) - 1;
		
		int bits = bitsToKeep & hashVal;
		//int bitBits = Integer.parseInt(Integer.toBinaryString(bitsToKeep));
		
		//System.out.print("[DEBUG] " + key + "'s full hashcode is: " + hashVal);
		bits = (int)Double.parseDouble((Integer.toBinaryString(bits)));
		//System.out.println("[DEBUG] hashVal bits is: " + bits);
		
		int hashValue = 0;
		int subHash = 0;
		int subCounter = 2;
		int subKey = key;
		//uncomment this for better hash function
		while( subKey > 0) {
			subHash = subKey % 5;
			if(subHash == 0) {
				hashValue += subCounter;
				subCounter++;
			} else {
				hashValue += subHash;
			}
			subKey = subKey / 5;
		}
		if(hashValue < 0) {
			hashValue *= -1;
		}
		//uncomment this line for debug	
		hashValue = Integer.parseInt(Integer.toBinaryString(hashValue));
		//System.out.println(key + "'s hashed value is now: " + hashValue);
		return hashValue; //uncomment this to return the actual hash function # that is created using subHash & subCounter
		/*
		//line below used for testing overflow -- all hash's for keys under 10 are the same
		if(key < 10) {
			//hashVal = Integer.parseInt("0001");	
		}
		return hashVal; //comment out above line for general hashVal to be returned
		*/
	}
	/**
	 * 
	 * @param key the full hash value of the original key
	 * @param numBitsToKeep the first N Bits to keep (2^n - 1) bits to keep
	 *  					(AKA the bucketBits for this bucket)
	 * @return
	 */
	private static int hashToNBits(int hashedKey, int numBitsToKeep) {
		int bitsToKeep = (int) Math.pow(2, numBitsToKeep) - 1;
		int hashedNBits = bitsToKeep & hashedKey;
		hashedNBits = (int)Double.parseDouble(Integer.toBinaryString(hashedNBits));
		return hashedNBits;
	}
	
	public void close() {
		//update roots and free... then close all 3 files
		//directory root does not need to be kept track of b/c shrinking dir should only add the last half of director to free list
		//overflow root does nto need to be kept track of...
		//bucket root does not need to be kept as you seek into bucket file at the bucket's addr from the dir file
		try {
			buckets.seek(0);
			buckets.writeInt(bucketSize);
			buckets.writeLong(bucketsFree);
			buckets.close();
			
			overflow.seek(0);
			overflow.writeLong(overflowFree);
			overflow.close();
			
			directory.seek(0);
			directory.writeInt(directoryBits);
			//directory.writeLong(dirFree); //delete
			directory.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	private int[] initKeys() {
		int[] keys = new int[bucketSize];
		for(int i = 0; i < bucketSize; i++) {
			keys[i] = 0;
		}
		return keys;
	}
	private long[] initAddrs() {
		long[] addrs = new long[bucketSize];
		for(int i = 0; i < bucketSize; i++) {
			addrs[i] = 0;
		}
		return addrs;
	}
	//repeat this method for each RAF and create an addFree() for each RAF...
	/**
	 *  if there's no free space, returns the end of file.
	 *  if there's something the free list, returns the next address in the list and updates free list
	 * @return the address of the next free space to be used
	 * @throws IOException
	 */
	private long getOverflowFree() throws IOException { //may need to update? corner cases
		//BTreeNode temp;
		long tempAddr;
		if (overflowFree == 0) { //nothing in the free list
			return overflow.length();
		} else { //something in the free list -- TEST
			overflow.seek(overflowFree);
			tempAddr = overflowFree;
			if(overflow.getFilePointer() > overflow.length() - 8) {
				overflowFree = 0;
			} else {
				overflowFree = overflow.readLong(); //updating overflowFree to the next free
			}
			//overflow.seek(tempAddr);
			//overflow.writeLong(overflowFree); //writing out the old free after this address 
			return tempAddr;
		}
	}
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	private long getBucketFree() throws IOException {
		long tempAddr;
		if (bucketsFree == 0) { //nothing in the free list
			return buckets.length();
		} else { //something in the free list -- TEST
			buckets.seek(bucketsFree);
			tempAddr = bucketsFree;
			if(buckets.getFilePointer() > buckets.length() - 8) {
				bucketsFree = 0;
			} else {
				bucketsFree = buckets.readLong();
			}
			//bucketsFree = buckets.readLong(); //updating overflowFree to the next free
			return tempAddr;
		}
	}
	/*
	 * this will be basically the same thing but need to create a bucket object instead of Row 
	 * and then initialize keys and addrs int he bucket to 0  & overflow and write to file
	 * */
	private long addFreeBucketsOrOverflow(long addr, boolean isBucketFile) throws IOException {
		long tempAddr=0; //save old free in tempAddr
		Bucket bucket = null;
		if (isBucketFile) {
			tempAddr = bucketsFree;
			bucketsFree = addr; //new head of free is the new address
			bucket = new Bucket(addr, false);
		} else { //in overflow
			tempAddr = overflowFree;
			overflowFree = addr; // new head of free is now the new address
			bucket = new Bucket(addr, true);
			//System.out.println("adding add " + addr + " to overflow free where last overflowFree was : " + tempAddr);
		}
		bucket.numKeys = 0;
		bucket.keys = initKeys();
		bucket.tableAddrs = initAddrs();
		bucket.overflowAddr = 0;
		bucket.writeBucket(addr); //making sure its all 0's in file
		
		if (isBucketFile) {
			buckets.seek(addr);
			buckets.writeLong(tempAddr); //set the (head of the free list) addr's next to be the old free
		} else {
			overflow.seek(addr);
			overflow.writeLong(tempAddr); //set the (head of the free list) addr's next to be the old free
		}
		return addr;
	}
	
	public void printInfo() {
		Bucket bucket = null;
		long buckAddr = 0;
		int bucketNum = 0;
		System.out.println("[SYSTEM]: Printing Directory Info Now");
		try {
			directory.seek(4);
			while(directory.getFilePointer() < directory.length()) {
				buckAddr = directory.readLong();
				if(buckAddr != 0 && buckAddr < buckets.length()) {
					bucket = new Bucket(buckAddr, false);
					System.out.println("at bucketAddr address: " + buckAddr);
					System.out.print("Keys for bucket #" + bucketNum + " with addresses are: ");
					for(int i = 0; i < bucketSize; i++) {
						System.out.print("[DEBUG]key:" + bucket.keys[i] + " addr:" + bucket.tableAddrs[i]);
					}
					System.out.println();
					while(bucket.overflowAddr != 0) { //print overflow info
						System.out.println("HAS OVERFLOW AT OVERFLOWADDR: " + bucket.overflowAddr);
						bucket = new Bucket(bucket.overflowAddr, true);
						for(int j = 0; j < bucketSize; j++) {
							System.out.print("[DEBUG]key:" + bucket.keys[j] + " addr:" + bucket.tableAddrs[j]);
						}
						System.out.println();
					}
					System.out.println();
					bucketNum++;
				} else { //print message saying bucket addr was not valid
					//System.out.println("[WARNING] bucket addr " + buckAddr + " at dir pos " + directory.getFilePointer() + " is not a valid Bucket");
				}	
			}//end while
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("overflowfree is: " + overflowFree);
	}
	/**
	 * 
	 */
	private void testSplitBucketsAndDoubleDirectory() {
		insert(1, 1);
		insert(3, 3);
		insert(2, 2);
		printInfo();
		insert(5, 5); //should cause doubleDir and split
		printInfo();
		insert(4, 4);
		insert(9, 9); //should cause doubleDir and split
		printInfo();
		
		close();
	}
	/**
	 * 
	 */
	private void testShrinkDirectory() {
		insert(1, 1);
		insert(3, 3);
		insert(2, 2);
		printInfo();
		insert(5, 5); //should cause doubleDir and split
		printInfo();
		insert(4, 4);
		insert(9, 9); //should cause doubleDir and split
		printInfo();
		
		remove(4); //regular remove
		
		remove(5); //should cause combine and shrink regardless of removing 4 b4 or not
		printInfo();
		
	}
	/**
	 * method to call after the first testOverflow is called and the old hash is closed
	 */
	private void testOverflowInsertAndRemove2() {
		
		insert(8, 8);
		remove(8);
		printInfo();
		remove(6);
		remove(7);
		printInfo();
		insert(6, 6);
		insert(7, 7);
		printInfo();
		insert(8, 8);
		printInfo();
		
		//System.out.println("overflowfree is : " + hash.overflowFree);
	}
	/**
	 * uncomment last line in hash(key) method to set hashval = Integer.parseInt("0001"); to cause 
	 * all key hashes to be the same and to cause overflow
	 */
	private void testOverflowInsertAndRemove() {
		insert(1, 1);
		insert(10, 10); //will insert into 0th bucket -- key hash = 01010
		insert(3, 3);
		insert(4, 4);//add to overflow w/ fixed hashCode
		printInfo();
		remove(4); //remove from overflow w/ fixed HashCode & should add to free list in overflwo
		printInfo();
		insert(4, 4);
		printInfo(); //overflow should start at the old overflow addr... doesnt
		insert(5, 5); //4 adn 5 in overflow together
		
		insert(6, 6);//own overflow bucket
		insert(7, 7); //all these are added to overflow
		
		insert(8, 8); //overflow's to its own overflow 
		printInfo();
		remove(8);
		printInfo();
		
		//this = new ExtHash("testFile");
	}
	//USED FOR TESTING PURPOSES
	public static void main(String[] args) {
		ExtHash hash = new ExtHash("testFile", 2);
		//hash.testShrinkDirectory();
		
		//hash.testSplitBucketsAndDoubleDirectory();
		
		hash.testOverflowInsertAndRemove();
		hash.close();
		hash = new ExtHash("testFile");
		hash.testOverflowInsertAndRemove2();
		
		hash.close();
		
		/*
		ExtHash hash = new ExtHash("testFile", 2);
		hash.insert(1, 1);
		hash.insert(3, 3);
		hash.insert(4, 4);//add to overflow w/ fixed hashCode
		hash.printInfo();
		hash.remove(4); //remove from overflow w/ fixed HashCode
		hash.printInfo();
		hash.insert(4, 4);
		hash.printInfo();
		
		hash.insert(2, 2);
		hash.insert(5, 5); //should cause doubleDir and split
		hash.insert(4, 4);
		hash.insert(9, 9); //should cause doubleDir and split
		hash.printInfo();
		//hash.remove(4); //regular remove
		
		hash.remove(5); //should cause combine and shrink regardless of removing 4 b4 or not
		hash.printInfo();
		
		hash.close();
		*/
		//insert like in quiz 5 to test doubling directory and then remove 1 to shrink...
	}
}
