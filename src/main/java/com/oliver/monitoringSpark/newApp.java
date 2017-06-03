package com.oliver.monitoringSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockStatus;
import org.apache.spark.storage.StorageStatus;

import com.oliver.monitoringSpark.listeners.CustomSparkListener;

import scala.Tuple2;
import scala.collection.JavaConversions;

public class newApp {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		
		conf.set("spark.memory.fraction", "1");
		conf.set("spark.memory.storageFraction", "1");

		SparkContext sc = new SparkContext("local[4]", "appName", conf);
		sc.addSparkListener(new CustomSparkListener());
		JavaSparkContext jsc = new JavaSparkContext(sc);

		printMemory();

		List<Integer> list = new ArrayList<Integer>();
		for (int j = 0; j < 10000000; j++) {

			list.add(j);
		}
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		final JavaRDD<String> rdd2 = rdd.map(new Function<Integer, String>() {

			public String call(Integer arg0) throws Exception {

				return arg0 + " id";
			}
		});

		rdd2.count();

		printMemory();

		String rdd3 = rdd2.reduce(new Function2<String, String, String>() {

			public String call(String arg0, String arg1) throws Exception {
				//printMemory();

				return arg0 + arg1;
			}
		});

		System.out.println(rdd3);

		printMemory();

		jsc.close();

	}

	public static void printMemory() {

		SparkContext sc = SparkContext.getOrCreate();

		scala.collection.Map<String, Tuple2<Object, Object>> memoryStatus = sc.getExecutorMemoryStatus();

		Map<String, String> executorEnvs = JavaConversions.mapAsJavaMap(sc.executorEnvs());
		for (String key : executorEnvs.keySet()) {
			String executorEnvsKey = executorEnvs.get(key);
			System.out.println("memoryEnv " + key + ": " + executorEnvsKey);

		}

		Map<String, Tuple2<Object, Object>> mapMemory = JavaConversions.mapAsJavaMap(memoryStatus);
		for (String memoryId : mapMemory.keySet()) {
			System.out.println("memoryId : " + memoryId);
			System.out.println("memory1 = " + (Long) mapMemory.get(memoryId).productElement(0) / (1024 * 1024));
			System.out.println("memory2 = " + (Long) mapMemory.get(memoryId).productElement(1) / (1024 * 1024));
		}

		for (StorageStatus status : sc.getExecutorStorageStatus()) {
			System.out.println("diskUsed = " + status.diskUsed() / (1024 * 1024));
			System.out.println("memUsed = " + status.memUsed() / (1024 * 1024));
			System.out.println("maxMem = " + status.maxMem() / (1024 * 1024));
			System.out.println("offHeadUsed = " + status.offHeapUsed() / (1024 * 1024));

			Map<BlockId, BlockStatus> mapBlocks = JavaConversions.mapAsJavaMap(status.blocks());

			for (BlockId blockId : mapBlocks.keySet()) {
				BlockStatus block = mapBlocks.get(blockId);

				System.out.println("-------------------------");
				System.out.println(blockId);
				System.out.println(block);
				System.out.println("-------------------------");
			}

			System.out.println("blockManagerId = " + status.blockManagerId());

		}

	}
}
