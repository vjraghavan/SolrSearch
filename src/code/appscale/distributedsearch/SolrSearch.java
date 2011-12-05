package code.appscale.distributedsearch;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import code.appscale.utils.*;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrSearch {
    final Logger logger = LoggerFactory.getLogger(SolrSearch.class);
    private SolrServer solrServer;
    private HealthChecker healthChecker;  
    private HTTPGetFile httpGetFile; 
    
    public SolrSearch() {
    	healthChecker = new HealthChecker();
        httpGetFile  = new HTTPGetFile();
    }
    
    /**
     * 
     * @param url
     */
    public boolean connect(String url) {
    	try {
    		logger.info("Creating a connection with URL " + url); 
			solrServer = new CommonsHttpSolrServer(url);
		} catch (MalformedURLException e) {
			logger.error("Malformed URL Exception"); 
			e.printStackTrace();
			return false;
		}
    	return true;
    }
    
    /**
     * 
     */
    public void disconnect() {
    	// solrServer.
    }
   	
    /**
     * 
     * @param f
     */
	public boolean index(String f) {
		String url = healthChecker.getHealthyMaster();
		// no healthy masters
		if(url == null) 
			return false;
		// not able to connect to healthy master ?!?
		
		url = "http://" + url; //  + "/solr";
		
		if(connect(url) == false) 
			return false;
		
		if(f.startsWith("http://")){
			logger.info("HTTP File to host " + url);
			return indexFile(f, false);
		}
		else {
			logger.info("LOCAL File to host " + url);
			return indexFile(f, true);
		}
		// return true;
	}
	
	/**
	 * indexFile function is for indexing given local or global file..
	 * along with the file contents it adds few other details like id
	 * id is of the form : username@hostname:filename
	 * this helps while retrieving the file / searching the file
	 * @param f
	 *  this is files names
	 * @param isLocal
	 * 	this parameter tells whether the file is local or global
	 */
	private boolean indexFile(String f,boolean isLocal) {
		String fileLoc = new String();
		if(!isLocal) {
			if(httpGetFile == null) {
				logger.error("httpGetFile is NULL");
				return false;
			}
			else {
				fileLoc = httpGetFile.download(f);
				if(fileLoc == null)
					return false;
			}
		}
		else {
			fileLoc = f;
		}
		
		File file = new File(fileLoc);
		if(!file.exists()) {
			logger.error("Specified File not found - " + file);
			return false;
		}
		String fileName   = file.getName();
		String filePath   = file.getAbsolutePath();
		String updateType = getUpdateType(getFileType(file.getName()));
		String hostName   = getHostName();
		String userName   = "AppScaleUser";
		
		ContentStreamUpdateRequest updateRequest = new ContentStreamUpdateRequest(updateType);
		try {
			updateRequest.addFile(file);
		} catch (IOException e) {
			logger.error("Error in File Update Request - " + updateRequest);
			e.printStackTrace();
			return false;
		}
		
		updateRequest.setParam("literal.id", 
				userName 
				+ "@" 
				+ hostName // + "," + solrServer
				+ ":" 
				+ f);
		updateRequest.setParam("fmap.content", "attr_content"); 
		updateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
		updateRequest.setAction(AbstractUpdateRequest.ACTION.OPTIMIZE, true, true);
	    
	    try{
	    	solrServer.request(updateRequest);
	    	System.out.println("File " + fileName + " Uploaded");
	    } catch(Exception e) {
	    	logger.error("Error while updating file to Solr ");
	    	e.printStackTrace();
	    	return false;
	    }
	    return true;
	}
	
	private String getHostName() {
		String hostName = new String();
		try {
			InetAddress addr = InetAddress.getLocalHost();
		    hostName = addr.getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return hostName;
	}

	/**
	 * returns the Suitable update URL for given fileType
	 * @param fileType
	 * @return
	 */
	private String getUpdateType(String fileType) {
		if(fileType.equalsIgnoreCase("json"))
			return "/update/json";
		else if(fileType.equalsIgnoreCase("csv"))
			return "/update/csv";
		else
			return "/update/extract";
	}

	/**
	 * Inputs a fileName and returns the fileType(extension) ..
	 * @param fileName - inputs the fileName in String
	 * @return fileType - in lowercase String
	 */
	private String getFileType(String fileName) {
		return fileName.substring(fileName.lastIndexOf('.') + 1).toLowerCase();
	}
	
	
	/**
	 * Searches for given query in SolrServer 
	 * @param query - String
	 */
	public ArrayList<String> search(String query, Boolean withContents) {
		String liveMaster = healthChecker.getHealthyMaster();
		String liveShards = healthChecker.getShards();
		String host = new String();
		if(liveMaster != null) {
			host = liveMaster;
		}
		else {
			System.out.println(liveShards);
			if(liveShards != null) {
				if(liveShards.indexOf(',') == -1) {
					// only one host alive
					host = liveShards;
				}
				else {
					// get first count 
					host = liveShards.substring(0,liveShards.indexOf(','));
				}
			}
			else {
				return null;
			}
		}
		ArrayList results = new ArrayList<String>();
		results = getSearchResults("http://" + host , query, withContents);
		disconnect();
		return results;
	}
	
	/**
	 * as of now it returns max 10 results !! 
	 * @param url
	 * @param query
	 * @param withContents
	 * @return
	 */
	public ArrayList<String> getSearchResults(String url, String query, Boolean withContents) {
		QueryResponse rsp = new QueryResponse();
		ArrayList<String> results = new ArrayList<String>();
		SolrQuery solrQuery= new SolrQuery();
		String shards = healthChecker.getShards(); 
		solrQuery.setParam("shards", shards);
		solrQuery.setQuery(query);
		connect(url);
		System.out.println(solrQuery);
		try {
			rsp = solrServer.query(solrQuery);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		if(rsp == null) {
			return results;
		}
		results.add(Long.toString(rsp.getElapsedTime()));
		if(rsp.getResults() == null) {
			return results;
		}
	    //
	    for(int i=0;i<rsp.getResults().size();i++) {
	    	//System.out.println((i+1) + "." + rsp.getResults().get(i).getFieldValue("id"));
	    	results.add(rsp.getResults().get(i).getFieldValue("id").toString());
	    }
	    return results;
	}
	
	public void delteAll() {
		
	}
	
	public static void main(String[] args) {
		SolrSearch solrSearch = new SolrSearch();
		// solrSearch.connect("http://localhost:8983/solr");
		//System.out.println(solrSearch.index("http://cs.ucsb.edu/~cs290c/projec"));
		System.out.println(solrSearch.search("*:*", false));
		System.out.println(solrSearch.search("*:*", false).size());
	}
}
