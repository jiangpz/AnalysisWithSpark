package advanced.chapter2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * 
 * @ClassName: TestNAStatCounter
 * @Description: 2.11 为计算概要信息创建可重用的代码 中的测试
 * @author: 蒋佩釗
 * @date: 2016年8月5日 上午11:04:48
 *
 */
public class TestNAStatCounter {

	public static void main(String[] args) {
		//测试NAStatCounter类
		NAStatCounter nas1 = new NAStatCounter(10.0);
		nas1.add(2.1);
		NAStatCounter nas2 = new NAStatCounter(Double.NaN);
		nas1.merge(nas2);
		System.out.println("nas1:");
		System.out.println(nas1.toString());
		
		//测试List统计
		NAStatCounter nas3 = new NAStatCounter();
		Arrays.asList(new Double[]{1.0, Double.NaN, 17.29}).forEach(d -> nas3.add(d));
		System.out.println("nas3:");
		System.out.println(nas3);
		NAStatCounter nas4 = new NAStatCounter();
		Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0}).forEach(d -> nas4.add(d));
		System.out.println("nas4:");
		System.out.println(nas4);

		//测试聚合
		List<NAStatCounter> list1 = Arrays.asList(new Double[]{1.0, Double.NaN, 17.29})
										 .stream()
										 .map(d -> new NAStatCounter(d))
										 .collect(Collectors.toList());
		System.out.println("list1:");
		for (NAStatCounter naStatCounter : list1) {
			System.out.println(naStatCounter);
		}
		List<NAStatCounter> list2 = Arrays.asList(new Double[]{Double.NaN, 15.39, 2.0})
				.stream()
				.map(d -> new NAStatCounter(d))
				.collect(Collectors.toList());
		System.out.println("list2:");
		for (NAStatCounter naStatCounter : list2) {
			System.out.println(naStatCounter);
		}
		List<Tuple2<NAStatCounter, NAStatCounter>> n0 = new ArrayList<Tuple2<NAStatCounter, NAStatCounter>>();
		for (int i = 0; i < list1.size(); i++) {
			n0.add(new Tuple2<NAStatCounter, NAStatCounter>(list1.get(i), list2.get(i)));
		}
		List<NAStatCounter> merged = n0.stream().map(p -> p._1.merge(p._2)).collect(Collectors.toList());
		System.out.println("merged:");
		merged.forEach(System.out::println);
	}

}
