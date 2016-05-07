package com.isikerhan.sparkexamples.ml.kmeans.runner;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.isikerhan.sparkexamples.ml.kmeans.KMeans;
import com.isikerhan.sparkexamples.ml.math.Vector;

public class KMeansRunner {

	private static String filePath;
	private static int maxNumberOfIterations = Integer.MAX_VALUE;
	private static int k;

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

		File out = new File(f.getParent().toString() + "/out.txt");
		out.createNewFile();
		
		PrintStream[] writers = new PrintStream[] {
				System.out,
				new PrintStream(out)
		};

		
		SparkConf config = new SparkConf().setAppName("k-means");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<Vector<?>> dataSet = sc.parallelize(dataSetList);

		KMeans kmeans = new KMeans(k, dataSet, sc);

		long t = System.currentTimeMillis();
		kmeans.initCentroids();

		int numberOfIterations;
		for (numberOfIterations = 0; kmeans.iterate()
				&& numberOfIterations < maxNumberOfIterations; numberOfIterations++)
			;

		for(PrintStream writer : writers){
			
			writer.println(String.format("Execution time: %d ms.", System.currentTimeMillis() - t));
			writer.println(String.format("Num of iterations: %d.", numberOfIterations));
			writer.println(kmeans.toString());
		}

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
