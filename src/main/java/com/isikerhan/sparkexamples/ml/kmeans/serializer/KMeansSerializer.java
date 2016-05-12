package com.isikerhan.sparkexamples.ml.kmeans.serializer;

import java.lang.reflect.Type;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.isikerhan.sparkexamples.ml.kmeans.Cluster;
import com.isikerhan.sparkexamples.ml.kmeans.KMeans;
import com.isikerhan.sparkexamples.ml.math.Vector;

public class KMeansSerializer implements JsonSerializer<KMeans>{

	@Override
	public JsonElement serialize(KMeans source, Type type, JsonSerializationContext context) {
		
		JsonObject obj = new JsonObject();
		
		JsonArray centroids = new JsonArray();
		for(Cluster c : source.getClusters().collect()){
			JsonObject cluster = new JsonObject();
			cluster.add("centroid", context.serialize(c.getCentroid(), Vector.class));
			cluster.addProperty("clusterNo", c.getClusterNo());
			centroids.add(cluster);
		}
		obj.add("centroids", centroids);
		
		JsonArray elements = new JsonArray();
		for(Entry<Vector<?>, Cluster> entry : source.getClusterMapping().collectAsMap().entrySet()){
			JsonObject element = new JsonObject();
			element.add("point", context.serialize(entry.getKey(), Vector.class));
			element.addProperty("clusterNo", entry.getValue().getClusterNo());
			elements.add(element);
		}
		obj.add("elements", elements);
		
		return obj;
	}

}
