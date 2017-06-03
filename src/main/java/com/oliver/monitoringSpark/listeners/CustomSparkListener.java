package com.oliver.monitoringSpark.listeners;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;

import scala.Tuple4;
import scala.collection.Seq;

public class CustomSparkListener implements SparkListener {

	public void onApplicationEnd(SparkListenerApplicationEnd arg0) {
		System.out.println("onApplicationEnd: " + arg0);

	}

	public void onApplicationStart(SparkListenerApplicationStart arg0) {
		System.out.println("onApplicationStart: " + arg0);
	}

	public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {
		System.out.println("onBlockManagerAdded: " + arg0);
	}

	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {
		System.out.println("onBlockManagerRemoved: " + arg0);

	}

	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {
		System.out.println("onBlockUpdated: " + arg0);

	}

	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		System.out.println("onEnvironmentUpdate: " + arg0);

	}

	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {
		System.out.println("onExecutorAdded: " + arg0);
	}

	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		System.out.println(arg0.execId());
		Seq<Tuple4<Object, Object, Object, TaskMetrics>> metrics = arg0.taskMetrics();
		for (int i = 0; i < metrics.size(); i++) {
			Tuple4<Object, Object, Object, TaskMetrics> tupla = metrics.apply(i);
			System.out.println("onExecutorMetricsUpdate: " + tupla._1());
			System.out.println("onExecutorMetricsUpdate: " + tupla._2());
			System.out.println("onExecutorMetricsUpdate: " + tupla._3());
			System.out.println("onExecutorMetricsUpdate: memoryBytesSpilled " + tupla._4().memoryBytesSpilled());
			System.out.println("onExecutorMetricsUpdate: diskBytesSpilled " + tupla._4().diskBytesSpilled());
			System.out.println("onExecutorMetricsUpdate: hostname " + tupla._4().hostname());
			System.out.println("onExecutorMetricsUpdate: accumulatorUpdates " + tupla._4().accumulatorUpdates());
			if (!tupla._4().inputMetrics().isEmpty()) {
				System.out.println("onExecutorMetricsUpdate: inputMetrics recordsRead "
						+ tupla._4().inputMetrics().get().recordsRead());
				System.out.println("onExecutorMetricsUpdate: inputMetrics readMethod "
						+ tupla._4().inputMetrics().get().readMethod());
				System.out.println("onExecutorMetricsUpdate: inputMetrics productPrefix "
						+ tupla._4().inputMetrics().get().productPrefix());

			}
			if (!tupla._4().outputMetrics().isEmpty()) {
				System.out.println("onExecutorMetricsUpdate: outputMetrics " + tupla._4().outputMetrics().get());
			}

		}

	}

	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {
		System.out.println("onExecutorRemoved: " + arg0);
	}

	public void onJobEnd(SparkListenerJobEnd arg0) {
		System.out.println("onJobEnd: " + arg0);
	}

	public void onJobStart(SparkListenerJobStart arg0) {
		System.out.println("onJobStart: " + arg0);
	}

	public void onStageCompleted(SparkListenerStageCompleted arg0) {
		System.out.println("onStageCompleted: " + arg0);
	}

	public void onStageSubmitted(SparkListenerStageSubmitted arg0) {
		System.out.println("onStageSubmitted: " + arg0);
	}

	public void onTaskEnd(SparkListenerTaskEnd arg0) {
		System.out.println("onTaskEnd: " + arg0);
	}

	public void onTaskGettingResult(SparkListenerTaskGettingResult arg0) {
		System.out.println("onTaskGettingResult: " + arg0);
	}

	public void onTaskStart(SparkListenerTaskStart arg0) {
		System.out.println("onTaskStart: " + arg0);
	}

	public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {
		System.out.println("onUnpersistRDD: " + arg0);
	}

}
