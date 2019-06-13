import java.io.*;
import java.util.*;

public class h2a {

		DBTable t1;  //stores keys with a first name and last name

		int t1Fields[] = {15, 30};

	private void insert_t1(String filename) throws IOException {
		System.out.println("Inserts into t1");
		BufferedReader b = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = b.readLine()) != null) {
			String fields[] = line.split(",");
			int key = new Integer(fields[0]);
			char f[][] = new char[2][];
			f[0] = Arrays.copyOf(fields[1].toCharArray(), 15);
			f[1] = Arrays.copyOf(fields[2].toCharArray(), 30);
			t1.insert(key, f);
		}
	}
	private void search(int val)  throws IOException {

		LinkedList<String> fields1;

		fields1 = t1.search(val);
		print(fields1, val);

	}


	private void print(LinkedList<String> f, int k) {
		if (f.size() == 0) { 
			System.out.println("Not Found "+k);
			return;
		}
		System.out.print(""+k+" ");
		for (int i = 0; i < f.size(); i++)
			System.out.print(f.get(i)+" ");
		System.out.println();
	}
			
	public h2a() throws IOException {
		int limit;

		t1 = new DBTable("f1", t1Fields, 2);


		//Insert data into t1
		insert_t1("faculty2.txt");

		for (int i = 0; i < 24; i++) {
			search(i);
		}

		//remove rows 2 and 22
		t1.remove(2);
		t1.remove(22);
		
		search(2);
		search(4);
		search(22);
		t1.close();

		t1 = new DBTable("f1");

		//Reuse table and insert more data into t1
		insert_t1("faculty1.txt");
		for (int i = 0; i < 24; i++) {
			search(i);
		}
		//remove all the odd rows
		for (int i = 1; i < 24; i = i+2) {
			t1.remove(i);
		}
		
		System.out.println("search for rows after removes");
		for (int i = 0; i < 24; i++) {
			search(i);
		}

		t1.close();
	}



	public static void main(String args[])  throws IOException  {
		new h2a();
	}
}