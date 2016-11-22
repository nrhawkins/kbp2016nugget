import com.typesafe.config.ConfigFactory

import java.io._
import java.nio.file.{Paths,Files}

import scala.io.Source


object CombineNuggetResults {


    val configFile = "nugCombine.conf"
    val config = ConfigFactory.load(configFile)

    val sys1File = config.getString("sys1-file")
    val sys2File = config.getString("sys2-file")
    val sys3File = config.getString("sys3-file")

    val outputFile = config.getString("output-file")
    val docIDList = config.getString("docID-list")    

    val systemID = config.getString("system-id")    
 

    case class Event(systemId:String, docid:String, mentionId:String, charSpanBegin:Int,
      charSpanEnd:Int, mention:String, eventType:String, realis:String, confidenceEventSpan:String,
      confidenceEventType:String, confidenceRealis:String)



    def main(args: Array[String]) {

      // ------------------------
      // Print config args
      // ------------------------
      System.out.println("Config File: " + configFile)
      System.out.println("Input: ")
      System.out.println("sys1Dir: " + sys1File)
      System.out.println("sys2Dir: " + sys2File)
      System.out.println("sys3Dir: " + sys3File)
      System.out.println("Output: ")
      System.out.println("outputFile: " + outputFile)
      System.out.println("DocIDs: ")
      System.out.println("docIDList: " + docIDList)
      System.out.println("\nReading input data..." + "\n")


      // ---------------------------------------
      // Read list of docIDs in Eval Corpus
      // ---------------------------------------
      val docIDs = {
        if(!Files.exists(Paths.get(docIDList))){
          System.out.println(s"Input file $docIDList doesn't exist! " + s"Exiting...")
          sys.exit(1)
        }
        //Read file, line by line
        Source.fromFile(docIDList).getLines().map(line => {
          line.trim
         })
      }.toList
      System.out.println("Num docIDs: " + docIDs.size)


      // ----------------------------------------------
      // Create outstream
      // ----------------------------------------------
      val outStream = new PrintStream(outputFile)                 

      // ----------------------------------------------
      // Read the Data
      // ----------------------------------------------

      val sys1Events = getSysEvents(sys1File)
      val sys2Events = getSysEvents(sys2File)
      val sys3Events = getSysEvents(sys3File)
        

      System.out.println("sys1: " + sys1Events.size)
      System.out.println("sys2: " + sys2Events.size)
      System.out.println("sys3: " + sys3Events.size)


      docIDs.foreach(d =>{

        System.out.println("Processing doc: " + d)
        val docID = d

        // -----------------------------------
        // get sys1 events for docID
        // -----------------------------------
        var sys1DocEvents = scala.collection.mutable.Set[Event]()
        val sys1DocEventsSet = sys1Events.filter(e => e.docid == docID)        
        for(e <- sys1DocEventsSet){
          sys1DocEvents.add(e)
        }
        //val sys1DocEvents = sys1Events.filter(e => e.docid == docID)
        //System.out.println("sys1: " + sys1DocEvents.size)

        // ------------------------------------
        // get sys2 events for docID
        // ------------------------------------
        var sys2DocEvents = scala.collection.mutable.Set[Event]()
        val sys2DocEventsSet = sys2Events.filter(e => e.docid == docID)
        for(e <- sys2DocEventsSet){
          sys2DocEvents.add(e)
        }
        //System.out.println("sys2: " + sys2DocEvents.size)

        // -------------------------------------
        // get sys3 events for docID
        // -------------------------------------
        var sys3DocEvents = scala.collection.mutable.Set[Event]()
        val sys3DocEventsSet = sys3Events.filter(e => e.docid == docID)
        for(e <- sys3DocEventsSet){
          sys3DocEvents.add(e)
        }
        //System.out.println("sys3: " + sys3DocEvents.size)

       // ----------------------------------------
       // sys Combined 
       // ----------------------------------------

       val matchOnly = false

       System.out.println("sys sizes: " + " 0" + ": " + sys1DocEvents.size + "," + sys2DocEvents.size + "," + sys3DocEvents.size)

       val sysCombined = combine2Systems(combine2Systems(sys1DocEvents, sys2DocEvents, matchOnly), sys3DocEvents, matchOnly)
       //val sysCombined = combine2Systems(sys1DocEvents, sys2DocEvents, matchOnly)

       //This would create a grand sum of the three systems
       //val sysCombined = sys1DocEvents ++ sys2DocEvents ++ sys3DocEvents

       //System.out.println("-----sysCombined: " + sysCombined.size)
       System.out.println("sys sizes: " + sysCombined.size + ": " + sys1DocEvents.size + "," + sys2DocEvents.size + "," + sys3DocEvents.size)


       // -------------------------------------
       // Write doc events
       // -------------------------------------
       printSystemOutput(outStream, systemID, docID, sysCombined)



      }) //foreach docID


      // ------------------------------
      // Close outstream
      // ------------------------------
      outStream.close()

      System.out.println("#-----------------------------")
      System.out.println("Finished: " + outputFile)
      System.out.println("Input1: " + sys1File)
      System.out.println("Input2: " + sys2File)
      System.out.println("Input3: " + sys3File)
      System.out.println("Num docIDs: " + docIDs.size)
      System.out.println("sys1 events: " + sys1Events.size)
      System.out.println("sys2 events: " + sys2Events.size)
      System.out.println("sys3 events: " + sys3Events.size)
      System.out.println("#-----------------------------")


    } //main



    def getSysEvents(sysFile:String):Set[Event] = {
    
      val sysEvents = {

          val fileName = sysFile
          if(!Files.exists(Paths.get(fileName))){
            System.out.println(s"Input file $fileName doesn't exist! " + s"Exiting...")
            sys.exit(1)
          }

          //Read file, line by line
          Source.fromFile(fileName).getLines().map(line => {

            val fields = line.trim.split("\t")
            if(fields.size >= 10){
              val eventOffsets = fields(3).split(",")
              var charSpanBegin = 0
              var charSpanEnd = 0
              if(eventOffsets.size >=2){
                charSpanBegin = eventOffsets(0).toInt
                charSpanEnd = eventOffsets(1).toInt
              }

              Event(fields(0),fields(1),fields(2),charSpanBegin,charSpanEnd,
                fields(4),fields(5),fields(6),fields(7),fields(8),fields(9))
            }
            else{
              //System.out.println("****getSysEvents: badLine")
              Event("badLine","fields(1)","fields(2)",-1,-1,"fields(4)",
                "fields(5)","fields(6)","fields(7)","fields(8)","fields(9)")
            }

          }).toSet.filter(r => r.systemId != "badLine")
        }

        return sysEvents
      
    }


    def combine2Systems(sys1:scala.collection.mutable.Set[Event], sys2:scala.collection.mutable.Set[Event],
      matchOnly:Boolean):scala.collection.mutable.Set[Event] = {

        var sysCombined = scala.collection.mutable.Set[Event]()

        (sys1.size, sys2.size) match {

          case p: Pair[Int,Int] if (p._1 >0 && p._2 >0) => {
            //System.out.println("!!!Both systems have values!!")
            if(!matchOnly) {sysCombined = compare2Systems(sys1,sys2)}
            else{ sysCombined = compare2SystemsMatch(sys1,sys2)}
          }
          case p: Pair[Int,Int] if (p._1 >0 && p._2 == 0) => {
            //System.out.println("!!!Only sys1 has values!!")
            if(!matchOnly) {sysCombined = sys1}
          }
          case p: Pair[Int,Int] if (p._1 == 0 && p._2 >0) => {
            //System.out.println("!!!Only sys2 has values!!")
            if(!matchOnly) {sysCombined = sys2}
          }

          case _ => System.out.println("Neither system has values!")        

        }

        return sysCombined

    }


    def compare2Systems(sys1:scala.collection.mutable.Set[Event],
      sys2:scala.collection.mutable.Set[Event]):scala.collection.mutable.Set[Event] = {

        var sysCombined = scala.collection.mutable.Set[Event]()
        var sys2MatchingEvents = scala.collection.mutable.Set[Event]()    
        
        var eventType = ""
        var eventBegin = 0 
        var eventEnd = 0

        for(event <- sys1){
           
            eventType = event.eventType
            eventBegin = event.charSpanBegin
            eventEnd = event.charSpanEnd
           
            sys2MatchingEvents = sys2.filter(s => matchingEvent(s,eventType,eventBegin,eventEnd))
            //System.out.println("####CB size: " + eventType + " " + sys2MatchingEvents.size) 
            sysCombined += event
            sys2MatchingEvents.foreach(s => {sys2 -= s})           
        }
        //add the non-matching events from sys2 to the combined
        sys2.foreach(s => {sysCombined += s})
        

        return sysCombined

    }


    def compare2SystemsMatch(sys1:scala.collection.mutable.Set[Event],
      sys2:scala.collection.mutable.Set[Event]):scala.collection.mutable.Set[Event] = {

        var sysCombined = scala.collection.mutable.Set[Event]()
        var sys2MatchingEvents = scala.collection.mutable.Set[Event]()    
        
        var eventType = ""
        var eventBegin = 0 
        var eventEnd = 0

        for(event <- sys1){
           
            eventType = event.eventType
            eventBegin = event.charSpanBegin
            eventEnd = event.charSpanEnd
           
            sys2MatchingEvents = sys2.filter(s => matchingEvent(s,eventType,eventBegin,eventEnd))
            //System.out.println("####CB size: " + eventType + " " + sys2MatchingEvents.size) 
            if(sys2MatchingEvents.size > 0){
              sysCombined += event
            }
        }

        return sysCombined

    }


    def matchingEvent(event:Event,eventType:String,eventBegin:Int, eventEnd:Int):Boolean = {
      
      var matches = false
    
        if(event.eventType == eventType && overlapOffsets(eventBegin,eventEnd,event.charSpanBegin,event.charSpanEnd)) {
          matches = true
        }

      return matches

    }

    def overlapOffsets(eventBegin1:Int,eventEnd1:Int,eventBegin2:Int,eventEnd2:Int):Boolean = {
    
      var overlap = false

      if( (eventBegin2 >= eventBegin1) && (eventBegin2 <= eventEnd1) ) overlap = true

      if( (eventEnd2 >= eventBegin1) && (eventEnd2 <= eventEnd1) ) overlap = true
   
      return overlap

    }


    def printSystemOutput(outStream:PrintStream, systemId:String, docID:String, 
      sysCombined:scala.collection.mutable.Set[Event]) = {


        // ------------------------
        // Write doc header
        // ------------------------
        outStream.println("#BeginOfDocument " + docID)

 
        // -----------------
        // For each event,
        // -----------------
        var eventCount = 0          

        sysCombined.foreach(l => {                    
        
          // ------------------------
          // Write out the events        
          // ------------------------

          eventCount += 1        

          outStream.println(systemId + "\t" + l.docid + "\t" + eventCount + "\t" + l.charSpanBegin + "," + 
            l.charSpanEnd + "\t" + l.mention + "\t" + l.eventType + "\t" + l.realis + "\t" + 
            l.confidenceEventSpan + "\t" + l.confidenceEventType + "\t" + l.confidenceRealis)          
        
        })

        // ------------------------
        // Write doc footer
        // ------------------------
        outStream.println("#EndOfDocument ")

    }


} //object 
