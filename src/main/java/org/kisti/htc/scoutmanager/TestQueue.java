package org.kisti.htc.scoutmanager;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class TestQueue {

    public static void main(String[] args) {

        Queue<String> qe=new LinkedList<String>();

        qe.add("b");
        qe.add("a");
        qe.add("c");
        qe.add("e");
        qe.add("d");


        Iterator it=qe.iterator();

        DebugLog.log("Initial Size of Queue :"+qe.size());

        while(it.hasNext())
        {
            String iteratorValue=(String)it.next();
            DebugLog.log("Queue Next Value :"+iteratorValue);
        }

        // get value and does not remove element from queue
        DebugLog.log("Queue peek :"+qe.peek());

        // get first value and remove that object from queue
        DebugLog.log("Queue poll :"+qe.poll());


        DebugLog.log("Final Size of Queue :"+qe.size());
    }
}