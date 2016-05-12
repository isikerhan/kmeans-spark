package com.isikerhan.sparkexamples.ml.kmeans.runner;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.isikerhan.sparkexamples.ml.kmeans.KMeans;
import com.isikerhan.sparkexamples.ml.kmeans.serializer.KMeansSerializer;
import com.isikerhan.sparkexamples.ml.math.Vector;

import scala.Tuple2;

public class KMeansRunner {

	private static String filePath;
	private static int maxNumberOfIterations = Integer.MAX_VALUE;
	private static int k;
	private static Gson gson = new GsonBuilder().registerTypeAdapter(KMeans.class, new KMeansSerializer()).create();

	public static void main(String[] args) throws IOException {

		setArgs(args);
		
		File f = new File(filePath);
		List<Vector<? extends Number>> dataSetList = null;
		try {
			dataSetList = Vector.fromCsv(f);
		} catch (IOException e) {

			String msg = String.format("%s: %s", e.getClass().getName(), e.getMessage());
			System.err.println(msg);
			System.exit(1);
		}

		String out = f.getParent().toString() + "/out";
		File outDir = new File(out);
		if(outDir.exists())
			FileUtils.deleteDirectory(outDir);
		
		SparkConf config = new SparkConf().setAppName("k-means");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<Vector<?>> dataSet = sc.parallelize(dataSetList);

		KMeans kmeans = new KMeans(k, dataSet, sc);

		long execTime = System.currentTimeMillis();
		kmeans.initCentroids();

		int numberOfIterations;
		for (numberOfIterations = 0; kmeans.iterate()
				&& numberOfIterations < maxNumberOfIterations; numberOfIterations++)
			;
		
		execTime = System.currentTimeMillis() - execTime;
		System.out.println(String.format("Execution time: %d ms", execTime));
		System.out.println(String.format("Number of iterations: %d", numberOfIterations));
	
		kmeans.getClusterMapping().mapToPair(t -> new Tuple2<>(t._2(), t._1())).groupByKey().saveAsTextFile(out);
		kmeans.getClusters().foreach(c -> System.out.println(c + ", " + c.getCentroid()));

		System.out.println("Result is saved as text file.");
		System.out.println(gson.toJson(kmeans, KMeans.class));
		
		sc.stop();
		System.exit(0);
	}
	
	private static void setArgs(String[] args) {

		String msg = "Error! Usage: kmeans <path_to_file> -k <num_of_clusters> [--maxNumOfIterations <max_num_of_iterations>]";
		
		if(args.length != 3 && args.length != 5){
			System.err.println(msg);
			System.exit(1);
		}
		
		filePath = args[0];
		Integer numOfClusters = null;
		Integer maxNumOfIterations = null;
		for(int i = 1; i < args.length; i++) {
			
			if("-k".equals(args[i].trim()))
				numOfClusters = Integer.parseInt(args[++i]);
			else if("--maxNumOfIterations".equals(args[i].trim()))
				maxNumOfIterations = Integer.parseInt(args[++i]);
		}
		
		if(numOfClusters == null){
			System.err.println(msg);
			System.exit(1);
		} else k = numOfClusters.intValue();
		
		if(maxNumOfIterations != null)
			maxNumberOfIterations = maxNumOfIterations.intValue();
	}
}
