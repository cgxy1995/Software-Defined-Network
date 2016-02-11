import java.io.*;
import java.net.*;
import java.lang.Thread;
import java.util.Timer;
import java.util.TimerTask;
import java.util.*;
// Main EchoServer class listens on port 5000 - accepts a client connection 
// Spawns a ClientEchoHandler thread to handle the client echo messages
// Schedules a timer immediately for 20 sec inactivity
public class Controller{
  public static Timer timer;
  public static void main (String[] args) {
    HashMap<Integer, Switch> switches;
    HashMap<Integer, HashMap<Integer, Path>> topology = new HashMap<Integer, HashMap<Integer, Path>>();
    ArrayList<ArrayList<Integer>> failLinks = new ArrayList<ArrayList<Integer>>();
    Utility utility = new Utility();
    HashMap<Integer, Socket> sockets = new HashMap<Integer, Socket>();
    String filename;
    if(args.length>1)
      filename = new String(args[1]);
    else
      filename = "config.txt";
    try {
      InetAddress address;
      int port;
      ServerSocket server = new ServerSocket(8888);
      Integer switchNum = new Integer(-1);
      
      WrapInt registered = new WrapInt(0);
      switches = readConfig(filename);
      switchNum = switches.size();
      //computePaths(switches);
      while (true) {
        Socket client = server.accept();
        registered.n++;
        address = client.getLocalAddress();
        port = client.getPort();
        timer = new Timer("Timer");
        System.out.println("Thread:### " + Thread.currentThread().getName() + " received client connection.. from ip: "+address+" port: "+port);
        ClientEchoHandler handler = new ClientEchoHandler(client, switches, topology, switchNum, sockets, failLinks, registered);
        handler.start();
      }
    }
    catch (Exception e) {
      System.err.println("Exception caught:" + e);
    }
  }
  public static HashMap<Integer, Switch> readConfig(String filename){
    HashMap<Integer, Switch>switches = new HashMap<Integer, Switch>();
    int switchNum=-1;
    try{
      FileReader fileReader = new FileReader(filename);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      switchNum = Integer.parseInt(bufferedReader.readLine());
      String line;
      while((line = bufferedReader.readLine()) !=null){
        String[] parts = line.split(" ");
        int fromSwitch = Integer.parseInt(parts[0]);
        int toSwitch = Integer.parseInt(parts[1]);
        int BW = Integer.parseInt(parts[2]);
        int delay = Integer.parseInt(parts[3]);
        if(!switches.containsKey(fromSwitch)){
          Switch aSwitch = new Switch(fromSwitch);
          aSwitch.addNeighbor(toSwitch);
          Collections.addAll(aSwitch.neighbors.get(toSwitch),BW,delay);
          switches.put(fromSwitch,aSwitch);
        }else{
          Switch aSwitch = switches.get(fromSwitch);
          aSwitch.addNeighbor(toSwitch);
          Collections.addAll(aSwitch.neighbors.get(toSwitch),BW,delay);
          switches.put(fromSwitch,aSwitch);
        }
        if(!switches.containsKey(toSwitch)){
          Switch aSwitch = new Switch(toSwitch);
          aSwitch.addNeighbor(fromSwitch);
          Collections.addAll(aSwitch.neighbors.get(fromSwitch),BW,delay);
          switches.put(toSwitch,aSwitch);
        }else{
          Switch aSwitch = switches.get(toSwitch);
          aSwitch.addNeighbor(fromSwitch);
          Collections.addAll(aSwitch.neighbors.get(fromSwitch),BW,delay);
          switches.put(toSwitch,aSwitch);
        }
      }
    }catch(FileNotFoundException ex){
      System.out.println(filename + " not found");
    }catch (Exception e) {
      System.err.println("Exception1 caught:" + e);
    }
    return switches;
  }
}
class Utility{
  public Utility(){}
  public HashMap<Integer, HashMap<Integer, Path>> computePaths(HashMap<Integer, Switch> switches, int switchNum, ArrayList<ArrayList<Integer>> failLinks){
    HashMap<Integer, HashMap<Integer, Path>> allPaths = new HashMap<Integer, HashMap<Integer, Path>>();
    for(Integer fromSwitch: switches.keySet()){
      HashMap<Integer, Path> onePath = new HashMap<Integer, Path>();
      for(Integer toSwitch: switches.keySet()){
          ArrayList<Path> possiblePaths = findPossiblePaths(fromSwitch, toSwitch, switchNum, switches, failLinks);
          ArrayList<Path> bestPaths = findWSPath(possiblePaths, switches);
          if(bestPaths.size()>0)
            onePath.put(toSwitch, bestPaths.get(0));
      }
      if(onePath.size()>0)
        allPaths.put(fromSwitch, onePath);
    }
    return allPaths;
  }
  public ArrayList<Path> findPossiblePaths(int start, int end, int switchNum, HashMap<Integer, Switch> switches, ArrayList<ArrayList<Integer>> failLinks){
    ArrayList<Path> paths = new ArrayList<Path>();
    Path path = new Path(switchNum);
    if(!switches.get(start).isActive()) return paths;
    path.add(start);
    Queue<Path> q = new LinkedList<Path>();
    q.offer(path);
    while(q.size()>0){
      int size = q.size();
      for(int i=0;i<size;i++){
        Path p = q.poll();
        Switch aSwitch = switches.get(p.current());
        for(Integer neighborId: aSwitch.neighbors.keySet()){
          int next = neighborId;
          boolean ifFailLink = false;
          //System.out.println("failinks size "+failLinks.size());
          for(ArrayList<Integer> failLink: failLinks){
            if(failLink.contains(next) && failLink.contains(aSwitch.id)){
              ifFailLink = true;
              break;
            }
          }
          if(!p.contains(next) && switches.get(next).isActive() && ifFailLink==false){
            Path newPath = new Path(p);
            newPath.add(next);
            if(aSwitch.neighbors.get(next).get(0)<p.bottleNeck) newPath.bottleNeck = aSwitch.neighbors.get(next).get(0);
            if(next == end){
              paths.add(newPath);
            }else{
              q.offer(newPath);
            }
          }
        }
      }
    }
    return paths;
  }
  public ArrayList<Path> findWSPath(ArrayList<Path> paths, HashMap<Integer, Switch> switches){
    ArrayList<Path> widestPaths = new ArrayList<Path>();
    ArrayList<Path> bestPaths = new ArrayList<Path>();
    if(paths.size()==0) return bestPaths;
    int max=0;
    for(int i=0;i<paths.size();i++){
      if(paths.get(i).bottleNeck>max)
        max = paths.get(i).bottleNeck;
    }
    for(int i=0;i<paths.size();i++){
      if(paths.get(i).bottleNeck==max)
        widestPaths.add(paths.get(i));
    }
    if(widestPaths.size()>1){
      int min=0;
      for(int i=0;i<widestPaths.size();i++){
        int len = calcPathLength(widestPaths.get(i), switches);
        widestPaths.get(i).pathLen = len;
        if (i==0) min = len;
        else{
          if(len<min)
            min = len;
        }
      }
      for(int i=0;i<widestPaths.size();i++){
        if(widestPaths.get(i).pathLen == min)
          bestPaths.add(widestPaths.get(i));
      }
    }else if(widestPaths.size()==1){
      int len = calcPathLength(widestPaths.get(0), switches);
      widestPaths.get(0).pathLen = len;
      bestPaths.add(widestPaths.get(0));
    }
    return bestPaths;
  }
  public int calcPathLength(Path path, HashMap<Integer, Switch> switches){
    int curr = path.path[0];
    int length=0;
    for(int i=1;i<path.index;i++){
      int next = path.path[i];
      for(Integer neighborId: switches.get(curr).neighbors.keySet()){
        if(neighborId == next){
          length += switches.get(curr).neighbors.get(neighborId).get(1);
          curr = next;
        }
      }
    }
    return length;
  }
  public int activeSwitches(HashMap<Integer, Switch> switches){
    int num=0;
    for(Switch aSwitch: switches.values()){
      if(aSwitch.isActive()) num++;
    }
    return num;
  }
  public void update_topology(HashMap<Integer, Switch> switches, HashMap<Integer, HashMap<Integer, Path>> topology,
   HashMap<Integer, Socket> sockets, int switchNum, ArrayList<ArrayList<Integer>> failLinks){
    topology = this.computePaths(switches, switchNum, failLinks);
        try{
          for(Integer fromSwitch: topology.keySet()){
            System.out.println("to id " + fromSwitch);
            Socket toSocket = sockets.get(fromSwitch);
            PrintWriter writer2 = new PrintWriter(toSocket.getOutputStream(), true);
            writer2.println("ROUTE_UPDATE");
            for(Integer toSwitch: topology.get(fromSwitch).keySet()){
              String thePath = "";
              System.out.print("from "+fromSwitch+" to " + toSwitch + " path: ");
              for(int i=0;i<topology.get(fromSwitch).get(toSwitch).index;i++){
                System.out.print(topology.get(fromSwitch).get(toSwitch).path[i]);
                thePath += topology.get(fromSwitch).get(toSwitch).path[i];
              }
              writer2.println("from "+fromSwitch+" to " + toSwitch + " path: " + thePath);
              System.out.println(", length: "+topology.get(fromSwitch).get(toSwitch).pathLen);
            }
            writer2.println("DONE");
          }
        }catch (Exception e) {
          System.err.println("Exception caught: client disconnected. in timer");
        }
  }
  public void delete_failLink(ArrayList<ArrayList<Integer>> failLinks, int from, HashMap<Integer, Switch> switches){
    /*if(switches.get(neighbor).isActive()){
      for(int i=0;i<failLinks.size();i++){
        if(failLinks.get(i).contains(neighbor) && failLinks.get(i).contains(from)){
          failLinks.remove(i);
          break;
        }
      }
    }*/
    for(int i=0;i<failLinks.size();i++){
        if(failLinks.get(i).get(0)==from){
          failLinks.remove(i);
          break;
        }
      }
  }
}


// The timer task class that is executed by the Timer thread
// Send 'bye' to close the client connection
class MyTimerTask extends TimerTask {
    Socket client;
    int id;
    HashMap<Integer, Switch> switches;
    HashMap<Integer, HashMap<Integer, Path>> topology;
    HashMap<Integer, Socket> sockets;
    ArrayList<ArrayList<Integer>> failLinks;
    Utility utility = new Utility();
    int switchNum=0;
    MyTimerTask(Socket passedClient, HashMap<Integer, HashMap<Integer, Path>> topology, HashMap<Integer, Switch> switches,
      HashMap<Integer, Socket> sockets, int id, int switchNum, ArrayList<ArrayList<Integer>> failLinks) {
        this.client = passedClient;
        this.switches = switches;
        this.topology = topology;
        this.sockets = sockets;
        this.id = id;
        this.switchNum = switchNum;
        this.failLinks = failLinks;
    }
    public void run() {
      System.out.println("Thread:### " + Thread.currentThread().getName() + " ###");
      System.out.println("Client "+ id +" didn't send TOPOLOGY_UPDATE in 20sec, marking it as inactive");
      switches.get(id).active = false;
      utility.update_topology(switches, topology, sockets, switchNum, failLinks);
    }
}

// ClientEchoHandler Thread that echoes the messages sent by the client
// It resets the timer in the timer thread
class ClientEchoHandler extends Thread {
  Socket client;
  Timer timer;
  HashMap<Integer, Switch> switches;
  HashMap<Integer, HashMap<Integer, Path>> topology;
  HashMap<Integer, Socket> sockets;
  ArrayList<ArrayList<Integer>> failLinks;
  int switchNum;
  WrapInt registered;
  ClientEchoHandler (Socket client, HashMap<Integer, Switch> switches
    , HashMap<Integer, HashMap<Integer, Path>> topology, int switchNum, HashMap<Integer, Socket> sockets, ArrayList<ArrayList<Integer>> failLinks, WrapInt registered) {
    this.client = client;
    this.switches = switches;
    this.topology = topology;
    this.switchNum = switchNum;
    this.sockets = sockets;
    this.timer = new Timer("timer");
    this.failLinks = failLinks;
    this.registered = registered;
  }

  public void run () {
    int id=0;
    boolean updated = false;
     Utility utility = new Utility();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
      PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
     
      while (true) {
        String line = reader.readLine();
        //writer.println("[echo] " + line);
        String[] parts = line.split(" ");
        //System.out.println("Thread: ### " + Thread.currentThread().getName() + " ### received echo: "+line);
        if(parts[0].equals("REGISTER_REQUEST")){
          id = Integer.parseInt(parts[1]);
          String ip = client.getInetAddress().toString();
          int port = client.getPort();
          System.out.println("Thread #" + Thread.currentThread().getName()+" received: REGISTER_REQUEST from switch ID " + id + ", ip: " + ip + ", port:" + port);
          Switch aSwitch = switches.get(id);
          sockets.put(id, client);
          String[] dum = ip.split("/");
          aSwitch.ip = new String(dum[1]);
          aSwitch.port = port;
          aSwitch.active = true;
          timer.schedule(new MyTimerTask(client, topology, switches, sockets, id, switchNum, failLinks), 20000); 
          writer.println("REGISTER_RESPONSE");
          for(Integer neighbor: aSwitch.neighbors.keySet()){
            if(switches.get(neighbor).isActive()) {
              writer.println(neighbor + " true " + aSwitch.ip + " " + port);
            }else{
              writer.println(neighbor +" false ");
            }
            
          }
          writer.println("DONE");
          //utility.update_topology(switches, topology, sockets, switchNum, failLinks);
        }else if(parts[0].equals("TOPOLOGY_UPDATE")){
          System.out.println("Thread #" + Thread.currentThread().getName() + " received: "+line);
          timer.cancel();
          timer = new Timer("timer");
          timer.schedule(new MyTimerTask(client, topology, switches, sockets, id, switchNum, failLinks), 20000);
          ArrayList<Integer> aliveNeighbors = new ArrayList<Integer>();
          for(int i=1;i<parts.length;i++){
            aliveNeighbors.add(Integer.parseInt(parts[i]));
          }
          //System.out.println("reg: "+registered.n +", switchNum:"+switchNum+" fail size "+failLinks.size());
          if(registered.n==switchNum){
            for(Integer neighbor: switches.get(id).neighbors.keySet()){
              boolean ifFailLink = false;
              for(ArrayList<Integer> failLink: failLinks){
                //System.out.println("fail link "+failLink.get(0)+"-"+failLink.get(1));
                if(failLink.contains(id) && failLink.contains(neighbor)){
                  ifFailLink = true;
                  break;
                }
              }
              if(!aliveNeighbors.contains(neighbor) && switches.get(neighbor).isActive() && ifFailLink==false){
                //indicates link failure
                System.out.println("detected link failure between "+id +" and "+neighbor);
                ArrayList<Integer> failLink = new ArrayList<Integer>();
                failLink.add(neighbor);
                failLink.add(id);
                failLinks.add(failLink);
              }
              //if(failLinks.size()>0)
                //utility.delete_failLink(failLinks, id, switches);
            }
          }
          utility.update_topology(switches, topology, sockets, switchNum, failLinks);
        }else if(parts[0].equals("KEEP_ALIVE")){
          int dest = Integer.parseInt(parts[2]);
          int src = Integer.parseInt(parts[1]);
          if(switches.get(dest).isActive()){
            Socket toSocket = sockets.get(dest);
            writer = new PrintWriter(toSocket.getOutputStream(), true);
            writer.println(line);
            //System.out.println("sent to id "+id+": "+line);
          }else{
            Socket toSocket = sockets.get(src);
            writer = new PrintWriter(toSocket.getOutputStream(), true);
            writer.println("DISCONNECTED "+dest);
          }
        }
      }
    }
    catch (Exception e) {
      System.out.println("client #"+id+" has disconnected, deleting from topology");
      topology.remove(id);
      sockets.remove(id);
      switches.get(id).active=false;
      timer.cancel();
      registered.n--;
      //System.out.println("before delete "+failLinks.size());
      utility.delete_failLink(failLinks, id, switches);
      //System.out.println("after delete "+failLinks.size());
    }
    finally {
      try { client.close(); }
      catch (Exception e ){ ; }
    }
  }
}
class Path{
  int path[];
  int size;
  int index;
  int bottleNeck = 9999;
  int pathLen = 0;
  public Path(int n){
    this.path = new int[n];
    this.size = n;
    this.index = 0;
  }
  public  Path(Path p){
    this.path = new int[p.size];
    this.path = Arrays.copyOf(p.path,p.path.length);
    this.size = p.size;
    this.index = p.index;
    this.bottleNeck = p.bottleNeck;
  }
  public void add(int swt){
    path[index++] = swt;
  }
  public int current(){
    return path[index-1];
  }
  public boolean contains(int n){
    for(int i=0;i<index;i++){
      if(path[i]==n) return true;
    }
    return false;
  }
  public int start(){
    return path[0];
  }
  public int end(){
    return path[index-1];
  }
}
class Switch{
  int id;
  int port;
  String ip;
  boolean active;
  HashMap<Integer, ArrayList<Integer>> neighbors;
  public Switch(int id){
    this.id = id;
    this.port = 0;
    this.active = false;
    this.neighbors = new HashMap<Integer, ArrayList<Integer>>();
  }
  public void addNeighbor(int neighbor){
    neighbors.put(neighbor, new ArrayList<Integer>());
  }
  public boolean isActive(){
    return this.active;
  }
}
class WrapInt{
  int n;
  public WrapInt(int n){
    this.n = n;
  }
}