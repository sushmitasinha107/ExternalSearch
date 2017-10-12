import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class Main {
	 private static final int N = 30;

	public  static void main(String args[]) throws IOException {
		// TODO Auto-generated constructor stub
		 BufferedWriter fbw = new BufferedWriter(new FileWriter("result123.txt"));
		 Map<Integer, HashSet<String>> map = new HashMap<Integer, HashSet<String>>();
		 BufferedReader fbr = new BufferedReader(new FileReader("access2.log"));
		 String  line = "";
		
		 while((line = fbr.readLine()) != null){
	        String values[] = line.split(",");		     
			int key = Integer.parseInt(values[1]);
			HashSet<String> setItems =  new HashSet<String> ();
			if (map.containsKey(key)) {
				setItems = map.get(key);
				setItems.add( values[2]);
				map.put(key, setItems);
			} else {
				setItems.add(values[2]);
				map.put(key, setItems);
			} 
		}
		 
		Set<Integer> resultSet = new TreeSet<Integer>();
	 	HashSet<String> set = new HashSet<String>();
		Iterator<Entry<Integer, HashSet<String>>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Integer, HashSet<String>> entry = (Entry<Integer, HashSet<String>>) iterator.next();
			
			set = (HashSet<String>) entry.getValue();
			if(set.size()>= N){
				resultSet.add((Integer) entry.getKey());
			}
		}
		
			
		for(Integer tempRes : resultSet){
			fbw.write( Integer.toString(tempRes));
			fbw.write('\n');
		 }
		fbw.close();

		 
	}
}
