package edu.washington.kbp;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.lang.StringBuffer;
import java.lang.Math;
import java.io.File;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.DocDateAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import edu.washington.common.KBPDocument;
import edu.washington.cs.figer.analysis.Preprocessing;

import edu.washington.nsre.LabelTriple;
import edu.washington.nsre.NewsSpikeSentencePredict;
import edu.washington.nsre.extraction.NewsSpikeRelationPrediction;
import edu.washington.nsre.figer.ParseStanfordFigerReverb;

import edu.washington.io.Reader;



public class KBPNuggetApp {


    //public static void main(String[] args){
    //    System.out.println("KBPNuggetApp: " );
    //}

    public static void doProcessNewsSpike(List<KBPDocument> documents, String outputFile){

        System.out.println("KBPNuggetApp: num docs " + documents.size());
        ParseStanfordFigerReverb sys = ParseStanfordFigerReverb.instance();
        Preprocessing.initPipeline();

        NewsSpikeSentencePredict nssp = new NewsSpikeSentencePredict();
        KBPTaxonomy kbpTaxonomy = new KBPTaxonomy();
        //File writeDir = new File(outputFile);
        BufferedWriter bw = null;
        try{

            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));

            int docCount = 0;

          for(KBPDocument document: documents){

            docCount += 1;
            String docId = document.getDocId();

            System.out.println("-------------------------------------------"); 
            System.out.println("doc #: " + docCount + " docId: " + docId); 
            System.out.println("-------------------------------------------"); 

            bw.write("#BeginOfDocument " + docId + "\n");


            try {

                List<CoreLabel> docTokens = new ArrayList<CoreLabel>();
                String docString = document.getCleanXMLString();
                Annotation annotation = new Annotation(docString);

                // --------------------------
                // Set docDate
                // --------------------------
                String docDate = document.getDocDate();
                System.out.println("DocDate: " + docDate);
                if(!docDate.equals("noDate")){
                    annotation.set(DocDateAnnotation.class, docDate);
                }
                // --------------------------
                // Annotate
                // --------------------------
                Preprocessing.pipeline.annotate(annotation);

                List<CoreMap> sentences = annotation.get(SentencesAnnotation.class);            
                 
                //System.out.println("KBPNuggetApp num sentences: " + sentences.size());

                // -------------------------------------
                // Collect the document tokens
                // -------------------------------------
                for(CoreMap sentence : sentences){
                    List<CoreLabel> tokens = sentence.get(TokensAnnotation.class);
                    docTokens.addAll(tokens);
                }

                //System.out.println("KBPEALApp num doc tokens: "  + docTokens.size());
 
                // -------------------------------
                // Call Newsspike
                // -------------------------------               
                List<NewsSpikeRelationPrediction> relations = nssp.predictKBP(annotation,sys);
                System.out.println("RELATIONS: " + relations.size() + " " + (relations == null));

                // --------------------------------- 
                // Examine Relations Output
                // ---------------------------------
                //Note: NewsSpikeRelationPrediction stores sentence token offsets, not sentence char offsets
                for(NewsSpikeRelationPrediction relation: relations){
                    int arg1Start = docTokens.get(relation.getArg1().getStartOffset()).beginPosition();
                    int arg1End = docTokens.get(relation.getArg1().getEndOffset()-1).endPosition()-1;
                    int arg2Start = docTokens.get(relation.getArg2().getStartOffset()).beginPosition();
                    int arg2End = docTokens.get(relation.getArg2().getEndOffset()-1).endPosition()-1;
                    //int verbStart = docTokens.get(relation.getVerb().getStartOffset()).beginPosition();
                    //int verbEnd = docTokens.get(relation.getVerb().getEndOffset()-1).endPosition()-1;
                    int verbStart = docTokens.get(relation.getVerb().getStartOffset()).beginPosition();
                    int verbEnd = docTokens.get(relation.getVerb().getEndOffset()).endPosition();

                    //String arg1CAS = testString.substring(arg1Start, arg1End);
                    //String arg2CAS = testString.substring(arg2Start, arg2End);

                    //CoreLabel arg2Token = docTokens.get(relation.getArg2().getStartOffset());
                    //String nnet = arg2Token.get(NormalizedNamedEntityTagAnnotation.class);
                    //System.out.println("arg2 NormalizedNamedEntityTag: " + nnet);

                    String arg1CAS = docString.substring(arg1Start, arg1End+1);
                    String arg2CAS = docString.substring(arg2Start, arg2End+1);

                    System.out.println("Verb Head: " + verbStart + ", " + verbEnd);

                    String verbStr = docString.substring(verbStart, verbEnd+1);                    
                    System.out.println("arg1: " + arg1CAS + " " + arg1Start + ":" + arg1End);
                    System.out.println("arg2: " + arg2CAS + " " + arg2Start + ":" + arg2End);
                    System.out.println("verb: " + verbStr + " " + verbStart + ":" + verbEnd);

                    System.out.println("relation: " + relation.getRelation());
                    System.out.println("arg1Type: " + relation.getArg1Type());
                    System.out.println("arg2Type: " + relation.getArg2Type());
                    System.out.println("confidence: " + relation.getConfidence());

                }

                // ------------------------------------
                // Create and Write Document Output
                // ------------------------------------

                int eventCount = 0;
                if(relations != null){

                   for(NewsSpikeRelationPrediction relation: relations){

                       //check if relation is in the set
                       List<LabelTriple> eventTypeRoles = kbpTaxonomy.newsSpikeToTAC2016EventTripleLabel(relation.getRelation());

                       if(eventTypeRoles!=null){
                   
                           for(LabelTriple labelTriple : eventTypeRoles){
                    
                               eventCount += 1;

                               //int verbStart = docTokens.get(relation.getVerb().getStartOffset()).beginPosition();
                               //int verbEnd = docTokens.get(relation.getVerb().getEndOffset()-1).endPosition();
                               int verbStart = docTokens.get(relation.getVerb().getStartOffset()).beginPosition();
                               int verbEnd = docTokens.get(relation.getVerb().getEndOffset()).endPosition();
                               String verbStr = docString.substring(verbStart, verbEnd);
                               //String eventType = labelTriple.getRelation().replace(".","_");
                               String eventType = labelTriple.getRelation();
                               String realis = "Actual";
                               String verbPhraseStart = docTokens.get(relation.getVerbPhrase().getStartOffset()).value();
                               if(verbPhraseStart.equals("will") || verbPhraseStart.equals("is")) realis = "Other";                                
                               System.out.println("verb phrase + head: " + relation.getVerbPhrase().getArgName() + ", " + relation.getVerb().getArgName());
                               System.out.println("token + realis: " + verbPhraseStart + ", " + realis);       

                               bw.write("WashingtonNS" + "\t" + docId + "\t" + eventCount + "\t" + verbStart + "," + verbEnd + 
                                   "\t" + verbStr + "\t" + eventType + "\t" + realis + "\t" + 
                                    relation.getConfidence() + "\t" + 
                                    relation.getConfidence() + "\t" + relation.getConfidence() + "\n");   

                           }
                       }

                   }

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            bw.write("#EndOfDocument" + "\n");

        } // for each doc

           bw.close();

       } catch (Exception e) {
           e.printStackTrace();    
       }


    } //doProcess


    public static String replaceIllegalCharacters(String string) {
        return string.replace("\r\n", " ").replace("\n", " ").replace("\t", " ");
    }


} //nugget app
