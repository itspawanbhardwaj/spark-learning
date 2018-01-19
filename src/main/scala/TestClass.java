import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestClass {

	public static void main(String args[]) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		String[] NQ = br.readLine().split("\\s+");

		int N = Integer.parseInt(NQ[0]);

		int Q = Integer.parseInt(NQ[1]);

		String str = br.readLine();

		if (str.length() != N) {
			throw new Exception("String length not equals to " + N);
		}

		for (int i = 0; i < Q; i++) {
			String[] queryArray = br.readLine().split("\\s+");

			int l = Integer.parseInt(queryArray[0]);
			int r = Integer.parseInt(queryArray[1]);

			String inputString = str.substring(l - 1, r);
			System.out.println(noOfStringsToBeDeleted(inputString));
		}

	}

	private static int noOfStringsToBeDeleted(String inputString) {
		HashMap<Character, Integer> map = characterCount(inputString);
		List<Integer> values = new ArrayList<Integer>(map.values());

		Map<Integer, Integer> occuranceCount = new HashMap<Integer, Integer>();

		for (Integer i : values) {
			if (!occuranceCount.containsKey(i)) {
				occuranceCount.put(i, i);
			} else {
				int value = occuranceCount.get(i);
				occuranceCount.put(i, value + i);
			}
		}

		List<Integer> occuranceList = new ArrayList<Integer>(occuranceCount.values());

		List<Integer> charDeleteList = getDeletingValueArray(occuranceList);
		return Collections.min(charDeleteList);
	}

	private static List<Integer> getDeletingValueArray(
			List<Integer> occuranceList) {
		List<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < occuranceList.size(); i++) {
			int value = sumOfElements(occuranceList) - occuranceList.get(i);
			list.add(value);
		}
		return list;
	}

	private static Integer sumOfElements(List<Integer> list) {
		int sum = 0;

		for (int i : list)
			sum = sum + i;

		return sum;
	}

	public static HashMap<Character, Integer> characterCount(String inputString) {

		HashMap<Character, Integer> charCountMap = new HashMap<Character, Integer>();

		char[] strArray = inputString.toCharArray();

		for (char c : strArray) {
			if (charCountMap.containsKey(c)) {
				charCountMap.put(c, charCountMap.get(c) + 1);
			} else {

				charCountMap.put(c, 1);
			}
		}

		return charCountMap;
	}
}
