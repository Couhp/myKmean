package hadoop.myKmean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Test {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File f = new File("data.txt");
		BufferedReader fin = new BufferedReader(new FileReader(f));
		String min = "";
		while((min = fin.readLine()) != null) {
			String[] in = min.split("[\\s\\t]");
			int y = 0;
			for (String i : in) {
				//System.out.println("line is : " + i);
				y++;
			}
			System.out.println(" :: " + y);
		}
	}

}
