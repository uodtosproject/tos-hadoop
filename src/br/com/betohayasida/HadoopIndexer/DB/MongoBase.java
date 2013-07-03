package br.com.betohayasida.HadoopIndexer.DB;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Base Class for MongoDB Classes
 *
 */
public class MongoBase {
	protected MongoClient mongoClient = null;
	protected DBCollection collection = null;
	protected DB db = null;
	
	/**
	 * Connect to the DB.
	 * @param name DB's name
	 */
	public boolean connect(String DBNAME, String COLLECTIONNAME){
		boolean connected = false;
		
		try {
			mongoClient = new MongoClient();
		} catch (UnknownHostException e) {
			//e.printStackTrace();
			System.out.println("The DB is offline");
		}
		if(mongoClient != null){
			db = mongoClient.getDB(DBNAME);
			collection = db.getCollection(COLLECTIONNAME);
			
			if(collection != null){
				connected = true;
			}
		}
		
		return connected;
	}
	
	/**
	 * Close the connection with the database
	 */
	public void close(){
		this.mongoClient.close();
	}
	
	/**
	 * Returns the DBCollection
	 * @return DBCollection object
	 */
	public DBCollection collection(){
		return this.collection;
	}

}
