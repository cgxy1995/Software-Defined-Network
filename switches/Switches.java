import java.io.*;
import java.net.*;
import java.lang.Thread;
import java.util.Timer;
import java.util.TimerTask;
import java.util.ArrayList;

public class Switches extends Thread{

    static Object lock = new Object();
    static String gswitchHostname = null;
    static int gport = 0;
    static String gswitchID = null;
    static boolean gcontr = false;
    
    static ArrayList<String> activeNeightbors = new ArrayList<String>();
    static String failureSimulation = "";
    static int failurePort = 0;
    static ArrayList<String> destID = new ArrayList<String>();
    //ArrayList<String> activeInfo = new ArrayList<String>();

    public Switches(String switchHostname, int port, String switchID, boolean contr){
        synchronized(lock){
            this.gswitchHostname = switchHostname;
            this.gport = port;
            this.gswitchID = switchID;
            this.gcontr = contr;
        }
    }

    public void run(){
        String switchHostname = null;
        int port = 0;
        String switchID = null;
        boolean contr = false;

        synchronized(lock){
            switchHostname = gswitchHostname;
            port = gport;
            switchID = gswitchID;
            contr = gcontr;
        }

        boolean talkToController = false;
        System.out.println ("Attemping to connect to host " +
            switchHostname + " on port "+port+".");

        Socket echoSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;
        boolean registered = false;

        try {
            synchronized(lock){
                echoSocket = new Socket(switchHostname,port);
            }
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(
                echoSocket.getInputStream()));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: " + switchHostname);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: " + switchHostname);
            System.exit(1);
        }

        try{
            String userInput;
            ArrayList<String> liveNeighbors = new ArrayList<String>();

            synchronized(lock){
                if(contr){
                    out.println("REGISTER_REQUEST "+switchID);
                    talkToController = true;
                    System.out.println("Sent: REGISTER_REQUEST "+switchID+" "+switchHostname+" "+port);
                }
                else{
                    for(int i = 0; i < destID.size(); i++){
                        if(failureSimulation.equals(destID.get(i))) continue;
                        out.println("KEEP_ALIVE "+switchID+" "+destID.get(i));
                        //System.out.println("KEEP_ALIVE to "+switchHostname+" @dest" + destID.get(i));
                    }

                }
            }

            long startTime = 0;
            Timer timer = new Timer("timer");
            
            while (true) {

                startTime = System.nanoTime();
                userInput = in.readLine();
                long endTime = System.nanoTime();

                //System.out.println("Client Received: " + userInput);

                if(userInput.contains("KEEP_ALIVE") && ((endTime - startTime)/1000000 < 2000)) {
                    //System.out.println("In KEEP_ALIVE");
                    if(!destID.contains(userInput.split(" ")[1]))
                        destID.add(userInput.split(" ")[1]);
                    //out.println("KEEP_ALIVE "+switchID+" "+userInput.split(" ")[1]);
                    
                    synchronized(lock){
                        if(!liveNeighbors.contains(userInput.split(" ")[1]))
                            liveNeighbors.add(userInput.split(" ")[1]);
                    }
                }
                else if(userInput.equals("ROUTE_UPDATE")){
                    System.out.println("Received ROUTE_UPDATE");
                    while(!(userInput=in.readLine()).equals("DONE")){
                        System.out.println(userInput);
                    }
                }
                else if(userInput.equals("REGISTER_RESPONSE")){
                    timer.schedule(new MyTimerTask(out, liveNeighbors),10000,10000);
                    timer.schedule(new Timer2(destID, failureSimulation, port, out, switchID),0,1000);
                    registered = true;
                    while(!(userInput=in.readLine()).equals("DONE")){
                        System.out.println(userInput+" RESPONSED.");
                        if(userInput.split(" ")[0].equals(failureSimulation)) continue;
                        synchronized(lock){
                            activeNeightbors.add(userInput);
                        }
                        //System.out.println(userInput);
                    }
                        //System.out.println(userInput);
                }
                else if(userInput.contains("DISCONNECTED")){
                    System.out.println(userInput);
                    liveNeighbors.remove(userInput.split(" ")[1]);
                    destID.remove(userInput.split(" ")[1]);
                    //System.out.println(liveNeighbors.size());
                    //for(int i = 0; i < liveNeighbors.size(); i++) System.out.println(liveNeighbors.get(i));
                    if(liveNeighbors.size() == 0){
                        System.out.println("Sent TOPOLOGY_UPDATE");
                        out.println("TOPOLOGY_UPDATE");
                    }
                    else{
                        String liveNeighborProtocol = "TOPOLOGY_UPDATE";
                        for(int i = 0; i < liveNeighbors.size(); i++){
                            liveNeighborProtocol += (" "+liveNeighbors.get(i));
                        }
                        out.println(liveNeighborProtocol);
                        liveNeighbors.clear();
                        System.out.println("Sent "+liveNeighborProtocol+" to controller");
                    }
                }

                
                if(in.equals("Bye")) break;
            }

            out.close();
            in.close();
            echoSocket.close();
        }
        catch (Exception e){
            System.err.println("Server Disconnected.");
        }
        
    }

    public static void main(String[] args) throws Exception {
        //The first argument is the switch ID.
        //The second argument is the controller hostname
        //The third argument is the controller port
        //If the number of the arguments contain 5 arguments and contains "-f", the fifty argument will be the failure neighbor ID.
  
        int controllerPort = Integer.parseInt(args[2]);
        String controllerHost = new String (args[1]);
        if(args.length == 5){
            failureSimulation = new String(args[4]);
        }

        //Socket switchSocket = new Socket(switchHostname,port);
        //ServerSocket server = new ServerSocket(Integer.parseInt(args[1]));

        Thread send = new Switches(controllerHost, controllerPort, new String(args[0]), true);
        send.start();

        while(true){
            synchronized(lock){
                if(!activeNeightbors.isEmpty()){
                    int switchPort = 0;
                    String switchHost = null;

                    for(int i = 0; i < activeNeightbors.size(); i++){
                        if(activeNeightbors.get(i).contains("false"))
                            continue;
                        System.out.println("Active neighbor: " + activeNeightbors.get(i));
                        if(activeNeightbors.get(i).split(" ")[0].equals(failureSimulation))
                            failurePort = Integer.parseInt(activeNeightbors.get(i).split(" ")[3]);
                        destID.add(activeNeightbors.get(i).split(" ")[0]);
                        switchHost = "localhost";
                        switchPort = Integer.parseInt(activeNeightbors.get(i).split(" ")[3]);
                        try{
                            Thread.sleep(50);
                        }
                        catch(Exception e){
                            System.err.println("Main thread cannot sleep.");
                        }
                    }
                    activeNeightbors.clear();
                }
            }
        }
    }
}

class Timer2 extends TimerTask{
    ArrayList<String> destID;
    String failureSimulation;
    int port;
    PrintWriter out;
    String switchID;
    Timer2(ArrayList<String> destID, String failureSimulation, int port, PrintWriter out, String switchID){
        this.destID = destID;
        this.failureSimulation = failureSimulation;
        this.port = port;
        this.out = out;
        this.switchID = switchID;
    }

    public void run(){
        
        for(int i = 0; i < destID.size(); i++){
            if(failureSimulation.equals(destID.get(i))){
                continue;
            }
            out.println("KEEP_ALIVE "+switchID+" "+destID.get(i));
            //System.out.println("KEEP_ALIVE "+switchID+" @dest " + destID.get(i));
        }

    }
}


class MyTimerTask extends TimerTask {
    PrintWriter out;
    ArrayList<String> liveNeighbors ;
    MyTimerTask(PrintWriter out, ArrayList<String> liveNeighbors) {
        this.out = out;
        this.liveNeighbors = liveNeighbors;
    }
    public void run() {
        //System.out.println(liveNeighbors.size());
        if(liveNeighbors.size() == 0)
            out.println("TOPOLOGY_UPDATE");
        else{
            String liveNeighborProtocol = "TOPOLOGY_UPDATE";
            for(int i = 0; i < liveNeighbors.size(); i++){
                liveNeighborProtocol += (" "+liveNeighbors.get(i));
            }
            out.println(liveNeighborProtocol);
            liveNeighbors.clear();
            System.out.println("Sent "+liveNeighborProtocol+" to controller");
        }
    }
}