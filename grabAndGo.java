/**
 * @author Tyon Davis
 * Date: 3/22/2023
 * CSCI 4201 Assignment 2 
 * Parallel and Distributed Database Systems Assignment 
 * 
 * 
 * You are going to write a Java program to find the optimal allocation of fragment replicas to sites in a 
 * distributed database. The program will read and parameter file which defines necessary parameter to 
 * decide how each fragment will be access in term of the expected cost of query and update, expected 
 * probability of query and update to each fragment. In the text file, the # indicates the comments of each 
 * parameter. 
 * Your program should be able to find and print optimal allocation for each fragment and display the 
 * overall cost of all possible replication as well. The optimal replication which is the lowest cost among all 
 * should be also printed. 
 * 
 * 
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;




public class grabAndGo {
	
	public static void optimalAllocation(double numFragment, double[][] probabilityOfEachFragQuery, double[][] probabilityOfEachFragUpdate, double[] eachFragQueryCost, double[] eachFragUpdateCost, double numSites) {
		
		//needed (variables)
		double minCost = Double.MAX_VALUE;
		double[][] optimalReplication = new double[(int) numFragment][(int) numSites];
		
		//(for loop) for each fragment
		for(int x = 0; x < numFragment; x++) {
			
			System.out.println("\nComputing optimal allocation for fragment " + x + "\n");
			
			//needed (variables)
			double[] currentReplication = new double[(int) numSites];
			double currentCost = 0;
			double[][] expectedCost = new double[(int) numSites][(int) numSites];
			
			//calculating the expected cost for each site to query or update this fragment
			for(int i = 0; i < numSites; i++) {
				for(int j = 0; j < numSites; j++) {
					if(i == j) {
						//if the site is the same as the site that has the fragment the cost is 0
						expectedCost[i][j] = 0;
					} else {
						//getting the expected cost
						expectedCost[i][j] = probabilityOfEachFragQuery[x][i] * eachFragQueryCost[x] + probabilityOfEachFragUpdate[x][j] * eachFragUpdateCost[x];
						
					}
				}
			}
			
			//for loop to go through each possible replication
			for(int i = 0; i <= numSites; i++) {
				for(int j = 0; j <= numSites - i; j++) {
					for(int k = 0; k <= numSites - i - j; k++) {
						int l = (int) (numSites - i - j - k);
						currentReplication[0] = i;
						currentReplication[1] = j;
						currentReplication[2] = k;
						currentReplication[3] = l;
						currentCost = 0;
						
						//calculating the expected cost for this replication
						for(int s = 0; s < numSites; s++) {
							for(int t = 0; t < numSites; t++) {
								currentCost += expectedCost[s][t] * currentReplication[s] * probabilityOfEachFragQuery[x][t];
							}
						}
						
						//checking for min cost and updating variable 
						if(currentCost < minCost) {
							minCost = currentCost;
							optimalReplication[x] = currentReplication.clone();
						}
						
						//printing out the current replication and its expected cost
						System.out.println("x=" + Arrays.toString(currentReplication) + ", expected cost = " + currentCost);
					}
				}
			}
			
			//printing out the optimal replication and its cost for this fragment
			System.out.println("\nFragment " + x + ":");
			System.out.println("Optimal Replication = " + Arrays.toString(optimalReplication[x]) + ", min Cost = " + minCost + "\n");
			minCost = Double.MAX_VALUE;
		}
		
	}
	
	
	//main line
    public static void main(String[] args)throws IOException {
    	
    	// needed (variables)
    	String passLine="";
    	double numFragment=0;
    	double [] eachFragQueryCost = null;
    	double [] eachFragUpdateCost=null;
    	double numSites=0;
    	double [][] probabilityOfEachFragQuery=null ;
    	double [][] probabilityOfEachFragUpdate=null;
    	int row=0;
    	int row1=0;
    	
    	
    	//(try-catch) statement informs user if the file is just not found 
        try {
        	
        	//inserting file into a variable named (file)
            File file = new File("C:/Users/tkdav/OneDrive/Desktop/CSCI4201.HW2.Inputs.txt");
            
            //(Scanner) gives us the ability to read the file 
            Scanner scanner = new Scanner(file);
            
            //(while loop) reads document line by line
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                
                
                //checking if the line is numbers- go to else or if words- store as our passLine would be our nonterminal 
                if (line.matches(".*[a-zA-Z].*")) {
                	passLine=line;
                	
                	}else {
	                	//storing data to its personal variables for flexibility for programmer 
		                if (passLine.equals("#Total number of fragment ")) {
		                	numFragment=Double.parseDouble(line);
		                	
		                }
		                if (passLine.equals("#Expected cost of a remote query to fragment i. ")) {
		                	//allows programmer to break string up by(,) and store as a double into an array
		                	eachFragQueryCost= Arrays.stream(line.split("[,]",0)).mapToDouble(Double::parseDouble).toArray();
		                	
		                }
		                if (passLine.equals("#Expected cost of a remote update to fragment i. ")) {
		                	eachFragUpdateCost= Arrays.stream(line.split("[,]",0)).mapToDouble(Double::parseDouble).toArray();
		                	
		                }
		                if (passLine.equals("#Number of sites ")) {
		                	numSites=Double.parseDouble(line);
		                	//print out number of sites
		                	System.out.println("Number of sites: "+(int)numSites);
		                	
		                	//storing our 2d arrays that we just got all data for
		                	probabilityOfEachFragQuery = new double[(int) numFragment][(int) numSites];
		                	probabilityOfEachFragUpdate = new double[(int) numFragment][(int) numSites];
		                }
		                if (passLine.equals("#Expected probability that fragment i is queried by site j. ")) {
		                	double set2Dline[]=Arrays.stream(line.split("[,]",0)).mapToDouble(Double::parseDouble).toArray();
		                	
		                	//setting each element to its proper address in array
		                	for (int i =0; i<(int)numSites;i++) {
		                		probabilityOfEachFragQuery[row][i]=set2Dline[i];	
		                	}
		                	row=row+1;
		                	
		            
		                	}
		                
		                if (passLine.equals("#Expected probability that fragment i is updated by site j. ")) {
		                	double set2Dline[]=Arrays.stream(line.split("[,]",0)).mapToDouble(Double::parseDouble).toArray();
		                	
		                	for (int i =0; i<(int)numSites-1;i++) {
		                		probabilityOfEachFragUpdate[row1][i]=set2Dline[i];
		                		
		                	}
		                	row1=row1+1;
	                	
	                	
	                }
	                
                }
                
                
            } 
            
            //(Function) passing all needed  to do calculations  
            optimalAllocation(numFragment,probabilityOfEachFragQuery, probabilityOfEachFragUpdate, eachFragQueryCost, eachFragUpdateCost, numSites);
            
            scanner.close();
        } 
        catch (FileNotFoundException e) {
            System.out.println("File not found.");
        }
        
    }
      
} 