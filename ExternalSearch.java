import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.Map.Entry;


public class ExternalSearch {
	public static final String DELIM = "\\|";
	public static Map<Integer, HashSet<String>> map = new HashMap<Integer, HashSet<String>>();
	public static int N = 0;
	public static final String inputFile = "access2.log";
	public static final String outputFile = "result.txt";


	public static void main(String[] args) throws IOException {
		String number = args[0];
		//N = Integer.parseInt(number);
		
		N = 30;
        long start = System.nanoTime();
        onDiskSearch(inputFile,outputFile);
        long endTime = System.nanoTime() - start;
        System.out.printf("Time taken %.2f seconds", endTime/1e9);
	}

	public static void onDiskSearch(String ipFile, String opFile) throws IOException {
		String inputfile = ipFile;
		List<File> l = sortInBatch(new File(inputfile)); //divide input file into chunks and sort based on UserId
		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String a1, String a2) {
				String a1Arr[] = a1.split("\\|");
				String a2Arr[] = a2.split("\\|");
					return a1Arr[0].compareTo(a2Arr[0]);
			}
		};
		mergeSearchedFiles(l, new File( opFile), comparator );
	}
	
	public static long estimateSizeOfBlocks(File inputFile) {
		long freemem = Runtime.getRuntime().freeMemory();
		long blocksize = freemem/2;
		return blocksize;
	}

	/*
	 divide input file into chunks and sort based on UserId and pre-process the batch file 
	 */
	public static List<File> sortInBatch(File file) throws IOException {
		List<File> files = new ArrayList<File>();
		BufferedReader fbr = new BufferedReader(new FileReader(file));
		long blocksize = estimateSizeOfBlocks(file);
		try {
			List<String> tmplist = new ArrayList<String>();
			String line = "";
			try {
				while (line != null) {
					long currentblocksize = 0;
					while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) {
						tmplist.add(line);
						currentblocksize += line.length(); 
					}
					files.add(sortAndSave(tmplist)); //preprocessing 
					tmplist.clear();
				}
			} catch (EOFException eof) {
				if (tmplist.size() > 0) {
					files.add(sortAndSave(tmplist));
					tmplist.clear();
				}
			}
		} finally {
			fbr.close();
		}
		return files;
	}

	/*
	 * 
	preprocess chunk of file to give a key value store with UserId and unique paths visited at a certain time
	Store it in temp file - ready for merging
	*/
	public static File sortAndSave(List<String> tmplist) throws IOException {
		File newtmpfile = File.createTempFile("processInBatch", "flatfile");
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
		 Map<Integer, HashSet<String>> map = new TreeMap<Integer, HashSet<String>>();
		try{
		 for(String line : tmplist){
	        String values[] = line.split(",");		     
			int key = Integer.parseInt(values[1]);
			HashSet<String> setItems =  new HashSet<String> ();
			if (map.containsKey(key)) {
				setItems = map.get(key);
				setItems.add(values[2]);
				map.put(key, setItems);
			} else {
				setItems.add(values[2]);
				map.put(key, setItems);
			} 
		}
		 
		Iterator<Entry<Integer, HashSet<String>>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Integer, HashSet<String>> entry = (Entry<Integer, HashSet<String>>) iterator.next();
			StringBuilder sb = new StringBuilder();
			sb.append(entry.getKey());
			sb.append("|");

			for(String s : (HashSet<String>) entry.getValue()){
				sb.append(s); sb.append(",");

			}
			sb.deleteCharAt(sb.length()-1);
			fbw.write( sb.toString());
			fbw.write('\n');
		}
		
		} 
		finally
		{
			fbw.close();
		}
		return newtmpfile;
	}

	
	/*
	 Merge the sorted temp files based on UserId and a file buffer for every temp file. 
	 Combine the path that the user has visited and check if the #of paths >= N
	 If yes, store the UserId in result.txt file
	 * 
	 */
	public static void mergeSearchedFiles(List<File> files, File outputfile, final Comparator<String> cmp)
			throws IOException {
		
		PriorityQueue<TempFileBuffer> pq = new PriorityQueue<TempFileBuffer>(
				new Comparator<TempFileBuffer>() {
					public int compare(TempFileBuffer i, TempFileBuffer j) {
						int x = cmp.compare(i.peek(), j.peek());
						if(x == 0){
							return ( i.fno - j.fno) ;
						}
						return x;
					}
				});		
		int i = 0;
		for (File f : files) {
			TempFileBuffer bfb = new TempFileBuffer(f);
			bfb.fno = i;
			i++;
			
			pq.add(bfb);
		}
		BufferedWriter fbw = new BufferedWriter(new FileWriter(outputfile));
		try {
			while (pq.size() > 0) {
				TempFileBuffer bfb = pq.poll();
				String r = bfb.pop();
				StringBuilder sb = new StringBuilder();
				sb.append(r);
				if (bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete(); //delete temp file
				} else 
				{
					pq.add(bfb); // adding back for reading more data
				}
				//process more data of same user id 
				while(pq.size() > 0){
					TempFileBuffer bfb1 = pq.poll();
					String s = bfb1.peek();
					String a1Arr[] = r.split("\\|");
					String a2Arr[] = s.split("\\|");
					if (a1Arr[0].compareTo(a2Arr[0]) == 0) {
						r = bfb1.pop();
						sb.append(",");sb.append(a2Arr[1]);
					}else{
					 	HashSet<String> setPaths = new HashSet<String>();
						String tempResArray[] = sb.toString().split("\\|");
						String tempPathArray[] = tempResArray[1].split(",");
						for(String strItem : tempPathArray){
							setPaths.add(strItem);
						}
						
						if(setPaths.size()>=N){
							fbw.write(tempResArray[0]); // write userID if  setPaths >= N
							fbw.newLine();
						}

						if (bfb1.empty()) {
							bfb1.fbr.close();
							bfb1.originalfile.delete();
						} else {
							pq.add(bfb1); // adding back for reading more data
						}
						break;
					}
					if (bfb1.empty()) {
						bfb1.fbr.close();
						bfb1.originalfile.delete();
					} 
					else {
						pq.add(bfb1); 
					}
				}
			}
		} finally {
			fbw.close();
			for (TempFileBuffer bfb : pq)
				bfb.close();
		}
	}	
}

class TempFileBuffer {
	public static int BUFFERSIZE = 2048;
	public BufferedReader fbr;
	public File originalfile;
	private String cache;
	private boolean empty;
	public int fno;
	public TempFileBuffer(File f) throws IOException {
		originalfile = f;
		fbr = new BufferedReader(new FileReader(f), BUFFERSIZE);
		reload();
	}
	public boolean empty() {
		return empty;
	}

	private void reload() throws IOException {
		try {
			if ((this.cache = fbr.readLine()) == null) {
				empty = true;
				cache = null;
			} else {
				empty = false;
			}
		} catch (EOFException eof) {
			empty = true;
			cache = null;
		}
	}

	public void close() throws IOException {
		fbr.close();
	}

	public String peek() {
		if (empty())
			return null;
		return cache;
	}

	public String pop() throws IOException {
		String answer = peek();
		reload();
		return answer;
	}

}
