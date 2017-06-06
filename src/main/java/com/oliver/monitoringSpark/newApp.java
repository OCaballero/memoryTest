package com.oliver.monitoringSpark;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockStatus;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageStatus;

import com.oliver.monitoringSpark.listeners.CustomSparkListener;

import scala.Tuple2;
import scala.collection.JavaConversions;

public class newApp {

	public static void main(String[] args) {

		Date date = new Date();

		SparkConf conf = new SparkConf();

		conf.set("spark.memory.fraction", "0");
		conf.set("spark.memory.storageFraction", "1");

		SparkContext sc = new SparkContext("local[4]", "appName", conf);
		sc.addSparkListener(new CustomSparkListener());

		JavaSparkContext jsc = new JavaSparkContext(sc);

		printMemory();

		List<ClaseTonta> list = new ArrayList<ClaseTonta>();
		for (int j = 0; j < 400000; j++) {
			list.add(new ClaseTonta(j));
		}

		JavaRDD<ClaseTonta> rdd = jsc.parallelize(list, 20000);
//		List<Tuple2<Integer, Iterable<Integer>>> a = rdd.mapToPair(new PairFunction<ClaseTonta, Integer, Integer>() {
//
//			public Tuple2<Integer, Integer> call(ClaseTonta t) throws Exception {
//				// TODO Auto-generated method stub
//				return new Tuple2(t.getNum() / 10, t.getNum());
//			}
//		}).groupByKey().collect();
		
//		System.out.println(a);

		 SQLContext sqlContext = new SQLContext(sc);
		 DataFrame dataframe =
		 sqlContext.createDataFrame(rdd,ClaseTonta.class);
		 System.out.println(dataframe.count());

		printMemory();
		jsc.close();

		Date date2 = new Date();

		System.out.println((date2.getTime() - date.getTime()) / 1000);

	}

	public static void printMemory() {

		SparkContext sc = SparkContext.getOrCreate();

		for (RDDInfo info : sc.getRDDStorageInfo()) {
			System.out.println(info);
		}

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
