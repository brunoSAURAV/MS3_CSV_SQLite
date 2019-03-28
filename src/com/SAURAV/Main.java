package com.SAURAV;


import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;


public class Main {

    public static void main(String[] args) {
        try {
            Class.forName("org.sqlite.JDBC");
            System.out.println("Load driver success");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:/Users/saurav/Desktop/MS3/ms3_csv_sqlite.db");

            //query to insert data into the table with 10 columns
            String query = "INSERT INTO ms3_csv_sqlite VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            //Prepare statement helps to execute parameterized query
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            //Fetching data form .csv file
            ArrayList<ms3Data> dataToBeRetrieved = getFromCsv("/Users/saurav/Desktop/MS3/ms3Interview (1).csv");

            //Inserting data we get from getFromCsv to our DB
            for (int i = 0; i < dataToBeRetrieved.size(); i++) {
                preparedStatement.setString(1, dataToBeRetrieved.get(i).getA());
                preparedStatement.setString(2, dataToBeRetrieved.get(i).getB());
                preparedStatement.setString(3, dataToBeRetrieved.get(i).getC());
                preparedStatement.setString(4, dataToBeRetrieved.get(i).getD());
                preparedStatement.setString(5, dataToBeRetrieved.get(i).getE());
                preparedStatement.setString(6, dataToBeRetrieved.get(i).getF());
                preparedStatement.setString(7, dataToBeRetrieved.get(i).getG());
                preparedStatement.setString(8, dataToBeRetrieved.get(i).getH());
                preparedStatement.setString(9, dataToBeRetrieved.get(i).getI());
                preparedStatement.setString(10, dataToBeRetrieved.get(i).getJ());

                //Executing the SQl statement and returning the results it produces
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*
        @params filePath: Give the exact location of file in your system
        @return ArrayList of Mountain State Solution, ms3Data object

     */
    public static ArrayList<ms3Data> getFromCsv(String filePath) throws IOException {

        //To read stream of byte oriented data
        FileInputStream fileInputStream = null;

        //Reads bytes and decodes them into character

        InputStreamReader inputStreamReader = null;

        // Read text from  character input stream
        BufferedReader bufferedReader = null;

        //Writing to faulty data to bad-data file
        String faultyDataFile = createNewFile();
        FileWriter fileWriter = new FileWriter(faultyDataFile);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        ArrayList<ms3Data> retrievedData = new ArrayList<ms3Data>();
        int numberOfFaultyRecords = 0;
        int numberOfSuccessfulRecords = 0;

        try {
            fileInputStream = new FileInputStream(filePath);
            inputStreamReader = new InputStreamReader(fileInputStream);
            bufferedReader = new BufferedReader(inputStreamReader);

            String fullLine = null;
            String[] wordArray = null;
            createNewFile();

            while ((fullLine = bufferedReader.readLine()) != null) {
                wordArray = fullLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                if(wordArray.length > 0 && wordArray.length == 10) {
                    retrievedData.add(new ms3Data(wordArray[0], wordArray[1], wordArray[2],
                            wordArray[3], wordArray[4], wordArray[5], wordArray[6], wordArray[7], wordArray[8], wordArray[9]));
                    numberOfSuccessfulRecords++;
                } else {
                    numberOfFaultyRecords++;
                    printWriter.println(fullLine);
                }
            }

        } catch (Exception e) {
            System.out.println("Read File Error");
            e.printStackTrace();
        }

        finally {
            // now we close all the buffers, readers, and writers in try and catch to maximize safety
            try {
                bufferedReader.close();
                inputStreamReader.close();
                fileInputStream.close();
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println( "Number of records that failed:" + numberOfFaultyRecords);
        System.out.println("Number of records that were succesful:" + numberOfSuccessfulRecords);
        System.out.println("Total number of records received:" + (numberOfFaultyRecords+numberOfSuccessfulRecords));
        return retrievedData;
    }

    /*
        Create new file for keeping error records.
        @Return filename to pass it to print writer
     */
    public static String createNewFile() {
        String nameOfFile = "/Users/saurav/Desktop/MS3/bad-data-" + new Date() + ".csv";
        try {
            File badDataFile = new File(nameOfFile);
            boolean fileCheck = badDataFile.createNewFile();
            if (fileCheck)
                System.out.println("Bad Data File has been created successfully");
	     else
            System.out.println("Bad Data File already exists at the specified location. Please choose another location or delete file.");

        } catch (IOException e) {
            System.out.println("Exception Occurred:");
            e.printStackTrace();
        }
        return nameOfFile;
    }
}
