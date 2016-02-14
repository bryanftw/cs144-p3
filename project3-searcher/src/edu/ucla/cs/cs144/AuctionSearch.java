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
                Statement s = conn.createStatement();
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

	private String escapeString(String input) {
		String output = input;		
		output.replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("&", "&amp;").replaceAll("\"", "&quot;").replaceAll("'", "&apos;");	
		return output;
	}
	private String formatDate(String input){
		SimpleDateFormat inputFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
		SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
		String output = "";
		try{
			Date parsedDate = inputFormat.parse(input);
			output += outputFormat.format(parsedDate);
		}catch(Exception e){System.out.println(e);}
		return output;
	}    

	public String getXMLDataForItemId(String itemId) {
        String result = "";

        try {
            Connection conn = DbManager.getConnection(true);

            Statement itemsStatement = conn.createStatement();
            ResultSet itemResult = itemsStatement.executeQuery("SELECT * FROM Items WHERE ItemID = " + itemId + ";");

            if (itemResult.next()) {
                // Item
                result += "<Item ItemID=\"" + itemId + "\">\n";

                // Name
                result += "<Name>" + itemResult.getString("Name") + "</Name>\n";

                // Category
                Statement itemCategoriesStatement = conn.createStatement();
                ResultSet itemCategoriesResult = itemCategoriesStatement.executeQuery("SELECT * FROM ItemCategories WHERE ItemID = " + itemId + ";");
                while (itemCategoriesResult.next()) {
                	result += "<Category>" + escapeString(itemCategoriesResult.getString("Category")) + "</Category>\n";
                }

                // Currently
                String currently = String.format("$%.2f",itemResult.getFloat("Currently"));
                result += "<Currently>" + currently + "</Currently>\n";

                // Buy Price
                String buyPrice = String.format("$%.2f", itemResult.getFloat("BuyPrice"));
                if(!buyPrice.equals("$0.00"))
                	result += "<Buy_Price>" + buyPrice + "</Buy_Price>\n";
            
            	// First Bid
            	String firstBid = String.format("$%.2f", itemResult.getFloat("FirstBid"));
            	result += "<First_Bid>" + firstBid + "</First_Bid>\n";

                // Number of Bids
                int numberOfBids = itemResult.getInt("NumberOfBids");
                result += "<Number_of_Bids>" + numberOfBids + "</Number_of_Bids>\n";

                // Bids
                if(numberOfBids == 0){
                	result += "<Bids />\n";
                }
                else{
					Statement bidsStatement = conn.createStatement();
					ResultSet bidsResult = bidsStatement.executeQuery("SELECT * FROM Bids WHERE ItemID = " + itemId + ";");
					result += "<Bids>\n";
					while (bidsResult.next()) {
						result += "<Bid>\n";
						String bidderID = escapeString(bidsResult.getString("BidderID"));
						String amount = escapeString(bidsResult.getString("Amount"));
	
						Statement bidderStatement = conn.createStatement();
						ResultSet bidderResult = bidderStatement.executeQuery("SELECT * FROM Bidders WHERE UserID = \"" + bidderID + "\";");
						
						if (bidderResult.next()) {	
							result += "<Bidder Rating=\"" + escapeString(bidderResult.getString("Rating")) + "\" UserID=\"" + bidderID + "\">\n";
							result += "<Location>" + escapeString(bidderResult.getString("Location")) + "</Location>\n";;
							result += "<Country>" + escapeString(bidderResult.getString("Country")) + "</Country>\n";
							result += "</Bidder>\n";
						}
	
						result += "<Time>" + escapeString(formatDate(bidsResult.getTimestamp("Time").toString())) + "</Time>\n";
						result += "<Amount>" + String.format("$%.2f",bidsResult.getFloat("Amount")) + "</Amount>\n";					
						result += "</Bid>\n";
					}
					result += "</Bids>\n";
                }
                
                // Get Seller entry for Location info
                String seller = escapeString(itemResult.getString("SellerID"));
                Statement sellerStatement = conn.createStatement();
                ResultSet sellerResult = sellerStatement.executeQuery("SELECT * FROM Sellers WHERE UserID = \"" + seller + "\";");
                
                
                if (sellerResult.next()) {
                	// Location, Latitude, Longitude
					String location = escapeString(sellerResult.getString("Location"));
					String latitude = escapeString(sellerResult.getString("Latitude"));
					String longitude = escapeString(sellerResult.getString("Longitude"));
					
					if(!latitude.equals("0.000000") && !longitude.equals("0.000000")){
						result += "<Location Latitude=\"" + latitude + "\" Longitude ='\"" + longitude + "\">" + location + "</Location>\n";
					}
					else {
						result += "<Location>" + location + "</Location>\n";
					}
	
					// Country, Started, Ends
					result += "<Country>" + escapeString(sellerResult.getString("Country")) + "</Country>\n";
					result += "<Started>" + formatDate(itemResult.getTimestamp("Started").toString()) + "</Started>\n";
					result += "<Ends>" + formatDate(itemResult.getTimestamp("Ends").toString()) + "</Ends>\n";
	
					// Seller
					result = result + "<Seller Rating=\"" + escapeString(sellerResult.getString("Rating")) + "\" UserID=\"" + seller + "\" />\n";
                }
                // Description
                result += "<Description>" + escapeString(itemResult.getString("Description")) + "</Description>\n";
                
                result += "</Item>";
            }          
            conn.close();
        } 
        catch (SQLException e) {
            System.out.println(e);
        }
        finally{
        	return result;
        }
	}

	public String echo(String message) {
		return message;
	}
}
