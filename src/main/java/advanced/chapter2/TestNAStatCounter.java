package advanced.chapter2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestNAStatCounter {

	public static void main(String[] args) {
		NAStatCounter nas1 = new NAStatCounter(10.0);
		nas1.add(2.1);
		NAStatCounter nas2 = new NAStatCounter(Double.NaN);
		nas1.merge(nas2);
		System.out.println(nas1.toString());
		
		NAStatCounter nas3 = new NAStatCounter();
		Arrays.asList(new Double[]{1.0, Double.NaN, 17.29}).forEach(d -> nas3.add(d));
		System.out.println(nas3);
		NAStatCounter nas4 = new NAStatCounter();
		Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0}).forEach(d -> nas4.add(d));
		System.out.println(nas4);

		List<NAStatCounter> list1 = Arrays.asList(new Double[]{1.0, Double.NaN, 17.29})
										 .stream()
										 .map(d -> new NAStatCounter(d))
										 .collect(Collectors.toList());
		for (NAStatCounter naStatCounter : list1) {
			System.out.println(naStatCounter);
		}
		List<NAStatCounter> list2 = Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0})
				.stream()
				.map(d -> new NAStatCounter(d))
				.collect(Collectors.toList());
		for (NAStatCounter naStatCounter : list2) {
			System.out.println(naStatCounter);
		}
//		list.stream().collect(Collectors.toList(NAStatCounter(d)));
	}

}
