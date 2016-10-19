package edu.washington.kbp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.washington.common.KBPDocument;
import edu.washington.io.Reader;
import edu.washington.utilities.KBPDocumentMaker;



public class RunKBP2016Nugget {


    public static void main(String[] args) {

        System.out.println("RunKBP2016Nugget");

        if(args.length != 3){
            System.out.println("Number of args: " + args.length + ", exiting!");
            System.exit(1);
        }

       try {

        // -------------------------------------
        // Set the args data
        // -------------------------------------
        String inputFileNamesFileName = args[0];
        String inputDirectory = args[1];
        String outputDirectory = args[2];

        System.out.println("Input File List: " + inputFileNamesFileName);
        System.out.println("Input Directory: " + inputDirectory);
        System.out.println("Output Directory: " + outputDirectory);

        // --------------------------------
        // Read the List of File Names
        // --------------------------------
        List<String> inputFileNames = Reader.getInputFileNames(inputFileNamesFileName);
        System.out.println("Number of Input Files: " + inputFileNames.size());

        // ----------------------------------------
        // Process each file to create a Document
        // ----------------------------------------
        List<KBPDocument> documents = new ArrayList<KBPDocument>();            
        int fileCount = 0;
        //for(int i=0; i<inputFileNames.size();i++){
        for(String fileName: inputFileNames){
            fileCount++;
            System.out.println("fileName: " + fileName);
            String docId = KBPDocumentMaker.getDocIdFromFileName(fileName);
            String docId2 = KBPDocumentMaker.getDocIdFromFileNameDotXml(fileName);
            System.out.println("docId: " + docId);
            System.out.println("docId2: " + docId2);
            KBPDocument document = new KBPDocument(docId);
            String inputFileName = inputDirectory + fileName;
            // ----------------------------
            // get doc string
            // ----------------------------
            String noTags = Reader.fileToNoXMLTagsString(inputFileName);
            //System.out.println("noTags: \n" + noTags); 
            document.setRawText(Reader.fileToString(inputFileName));
            document.setCleanXMLString(noTags);
            //document.setCleanXMLString(Reader.cleanXMLString(document.getRawText()));
            System.out.println("noTags length: " + noTags.length());
            System.out.println("rawText length: " + document.getRawText().length());
            // ----------------------------
            // get/set doc date
            // ----------------------------
            document.setDocDate(KBPDocumentMaker.getDocDateFromFileName(fileName));
            // ----------------------------
            // parse xml
            // ----------------------------
            System.out.println("inputFileName: " + inputFileName);

            //System.out.println("Document: " + "\n" + document.getRawText());
            //System.out.println("Document: " + "\n" + document.getCleanXMLString());
            if(document != null) documents.add(document);
        } 
        System.out.println("Number of input files: " + fileCount);


        // -----------------------------------------
        // Call NewsSpike with the documents
        // -----------------------------------------
        KBPNuggetApp.doProcessNewsSpike(documents, outputDirectory);

     
     } catch (Exception e) {
            e.printStackTrace();
     }



    } //main


}


