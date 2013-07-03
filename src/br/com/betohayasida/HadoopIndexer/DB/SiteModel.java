package br.com.betohayasida.HadoopIndexer.DB;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class SiteModel extends MongoBase {
	private String DBNAME = "crawler";
	private String COLLECTIONNAME = "sites";

	/**
	 * Connects to the DB
	 * @return True if the connection is successful
	 */
	public boolean connect(){
		return super.connect(DBNAME, COLLECTIONNAME);
	}
	
	/**
	 * Retrieves the last added entries
	 * @param limit Limit of entries to be retrieves
	 * @return List<Site> object
	 */
	public List<Site> getSites() {
		List<Site> results = new ArrayList<Site>();
		
		if(this.connect()){
			BasicDBObject sortPredicate = new BasicDBObject();
			
			DBCursor cursor = this.collection().find().sort(sortPredicate);
	
			while(cursor.hasNext()) {
				DBObject result = cursor.next();
				
				Site site = new Site();
				site.setName((String) result.get("name"));
				site.setVisitedOn((String) result.get("visitedOn"));
				site.setUrl((String) result.get("url"));
				site.setDomain((String) ((result.get("domain")!=null) ? result.get("domain") : site.getUrl()));
				results.add(site);
				
			}
			this.close();
		}
		
		return results;
	}

}
