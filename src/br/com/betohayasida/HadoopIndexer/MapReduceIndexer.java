package br.com.betohayasida.HadoopIndexer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.bson.*;

import br.com.betohayasida.HadoopIndexer.DB.*;
import br.com.betohayasida.SolrIndexer.SolrIndexer;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class MapReduceIndexer {
	
	static Logger logger = Logger.getLogger(MapReduceIndexer.class);
	static Appender myAppender = new ConsoleAppender(new SimpleLayout());
	static List<String> currentParents = new ArrayList<String>();
	
	private static void init(){

	    BasicConfigurator.configure();
	    logger.setLevel(Level.ALL);
		myAppender.setLayout(new SimpleLayout());
	    logger.addAppender(myAppender);

	}
	
	private static void loadSites(){
		List<Site> sites = new ArrayList<Site>();
		SiteModel siteModel = new SiteModel();
		if(siteModel.connect()){
			sites = siteModel.getSites();
		}
		for(Site site : sites){
			currentParents.add(site.getName() + "|" + site.getVisitedOn());
		}
	}
	
    public static class DocMapper extends Mapper<Object, BSONObject, Text, Text> {
        
        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException{
        	
            JSONObject record = JSONObject.fromObject(value);
            JSONObject doc = new JSONObject();
            doc.put("id", record.get("name") + "|" + record.get("retrievedOn"));
            doc.put("parent", record.get("parent"));
            doc.put("name", record.get("name"));
            doc.put("text", record.get("text"));
            doc.put("title", record.get("title"));
            doc.put("url", record.get("url"));
            if(currentParents.contains(doc.get("parent"))){
                doc.put("status", "current");
            	context.write(new Text("current"), new Text(doc.toString()));
            } else {
                doc.put("status", "archived");
            	context.write(new Text("archived"), new Text(doc.toString()));
            }
        }
    }
    
    public static class DocReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	SolrIndexer indexer = new SolrIndexer();
        	indexer.open(null);
        	for(Text value : values){
        		JSONObject doc = JSONObject.fromObject(value.toString());
        		indexer.add(doc);
            	context.write(key, new Text(doc.get("name").toString()));
        	}
	        indexer.commit();
        }
        
    }
    
    public static void main( String[] args ) throws Exception{
    	
    	init();
    	
    	loadSites();
    	
        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI(conf, "mongodb://127.0.0.1:27017/crawler.pages");
        MongoConfigUtil.setCreateInputSplits(conf, false);
        
        final Job job = new Job(conf, "mapreduce");
        FileOutputFormat.setOutputPath(job, new Path("/Users/rkhayasidajunior/Dev/javaWorkspace/Hadoop/output" + System.currentTimeMillis()));
 	   
        job.setJarByClass(MapReduceIndexer.class);

        job.setMapperClass(DocMapper.class);
        job.setReducerClass(DocReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MongoInputFormat.class);
        //job.setOutputFormatClass(MongoOutputFormat.class);

        System.exit(job.waitForCompletion( true ) ? 0 : 1);
    }
}
