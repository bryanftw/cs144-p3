package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
    //private      
         
   // public
    
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) {
        //Call spatialSearch with no region
        return spatialSearch(query, null, numResultsToSkip, numResultsToReturn);
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		// Create a region filter
        FieldCacheTermsFilter regionFilter = null;
		if (region != null)
        {
            //open a connection and do spatial search
            double lx, rx, ly, ry;
            lx = region.getLx();
            rx = region.getRx();
            ly = region.getLy();
            ry = region.getRy();
            //make the query string
            String spatialPoly = String.format("Set @poly = 'Polygon((%f %f, %f %f, %f %f, %f %f))';" , lx, ly, lx, ry, rx, ry, rx, ly);
            //open database connection
            Connection conn = null;
            try {
                conn = DbManager.getConnection(true);
            } catch (SQLException ex) {
                System.out.println(ex);
            }
            try {
                Statement s = conn.createStatement() ;
                ResultSet rs = s.executeQuery(spatialPoly);
                rs = s.executeQuery( "SELECT ItemID, AsText(Coordinates) FROM ItemCoordinates WHERE MBRContains(GeomFromText(@poly),Coordinates);");
                ArrayList<String> list = new ArrayList<String>();
                while( rs.next() ){
                    String id = rs.getString("ItemID");
                    list.add(id);
                }
                String[] itemsInRegion = list.toArray(new String[list.size()]);
                regionFilter = new FieldCacheTermsFilter("id", itemsInRegion);
            }
            catch (SQLException ex)
            {System.out.println(ex);}
            // close the database connection
            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println(ex);
            }


            
        }
        
        //Search
        try {
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/index1"))));
            QueryParser parser = new QueryParser("content", new StandardAnalyzer());
            Query parsedQuery = parser.parse(query);
            TopDocs results = searcher.search(parsedQuery, regionFilter, numResultsToReturn + numResultsToSkip);
            
            ScoreDoc[] hits = results.scoreDocs;
            
            if (hits.length - numResultsToSkip < numResultsToReturn)
                numResultsToReturn = hits.length;
            SearchResult[] returnResults = new SearchResult[numResultsToReturn];
            
            for(int i=0; i < numResultsToReturn; i++)
            {
                int docId =  hits[i+numResultsToSkip].doc;
                Document d = searcher.doc(docId);
                
                returnResults[i] = new SearchResult(d.get("id"), d.get("name"));
            }
            
            return returnResults;
        }
        catch (IOException ioe)
        {
            System.out.println(ioe);
        }
        catch (ParseException ex)
        {
            System.out.println(ex);
        }
        
        return new SearchResult[0];
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
