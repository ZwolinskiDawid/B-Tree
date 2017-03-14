package btree;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interface {
    
    private BTree btree;
    Scanner scanner;
    private MyRandom random;
    
    String fileNameToCount;
    String ext;
    
    
    public Interface(String fileNameToCount, String ext) {
        
        btree = null;
        scanner = new Scanner(System.in);
        this.random = new MyRandom();
        
        this.fileNameToCount = fileNameToCount;
        this.ext = ext;
        
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {   
        
        String fileNameToCount = "test";
        String ext = ".txt";
        
        FileInputStream is = new FileInputStream(new java.io.File(fileNameToCount + ext));
        //System.setIn(is);
        
        Interface tui = new Interface(fileNameToCount, ext);        
        
        try {
            while(true) {
            
                tui.getCommand();
            
            }
        } catch(ExitException e) { }
        finally {
            tui.btree.printFileSizes();
        }
        
    }
    
    private void getCommand() throws ExitException, FileNotFoundException, IOException {
        
        String command = null;
        if(this.scanner.hasNextLine())
            command = this.scanner.nextLine() + " ";
        else
            throw new ExitException();
        
        Pattern pattern = Pattern.compile("\\ ");
        Matcher matcher = pattern.matcher(command);
        
        if(command.compareTo(" ") != 0 && matcher.find()) {
            
            String cmd = command.substring(0, matcher.start());

            //wyjście z programu
            if(cmd.compareTo("exit") == 0) {
                throw new ExitException();
            }            
            
            this.getParams(cmd, command);
              
        } else {
            
            System.out.println("Incorrect command!");
            
        }
    }
    
    private void getParams(String command, String params) throws FileNotFoundException, IOException {
        
        Map<String, String> map = new HashMap<>();
        
        Pattern pattern = Pattern.compile("-[a-z]\\ [\\w.]*");
        Matcher matcher = pattern.matcher(params);
        
        while(matcher.find()) {
            
            String cmd = matcher.group();
            Matcher paramMatcher = Pattern.compile("\\ ").matcher(cmd);
            
            if(!paramMatcher.find() || 
                    map.putIfAbsent(cmd.substring(0, paramMatcher.start()), 
                            cmd.substring(paramMatcher.end())) != null) {
                
                System.out.println("Incorrect command!");
                return;
                
            }
        }
        
        this.execute(command, map);
    }
    
    private void execute(String command, Map<String, String> params) throws FileNotFoundException, IOException {
        
        switch(command) {
                
                case "init":
                    this.executeInit(params);
                    break;
                case "insert":
                    if(this.isBTreeInit())
                        this.executeInsert(params);
                    break;
                case "update":
                    if(this.isBTreeInit())
                        this.executeUpdate(params);
                    break;
                case "read":
                    if(this.isBTreeInit())
                        this.executeRead(params);
                    break;
                case "remove":
                    if(this.isBTreeInit())
                        this.executeRemove(params);
                    break;
                default:
                    System.out.println("Incorrect command!");                
        }        
    }
    
    private void executeInit(Map<String, String> params) throws FileNotFoundException, IOException {
        String index = null, records = null;
        int d = -1;
        
        System.out.println("init");
        
        for(String key : params.keySet()) {
            
            System.out.print(key + "," + params.get(key));
            
            switch(key) {
                    
                case "-i":
                    index = params.get(key);
                    break;
                case "-r":
                    records = params.get(key);
                    break;
                case "-d":
                    d = Integer.parseInt(params.get(key));
                    break;
                default:
                    System.out.print("Incorrect param!");                  
            }
                
            System.out.println();
            
        }
        
        if(index != null && records != null && d != -1)        
            this.btree = new BTree(index, records, d, this.fileNameToCount + "Count" + this.ext);
        else
            System.out.println("Too few params!");
    }
    
    private void executeInsert(Map<String, String> params) throws IOException {
        int keyToInsert = -1;
        int typeOfPrinting = 0;
        
        System.out.print("insert ");
        
        for(String key : params.keySet()) {
            
            System.out.print(key + ": " + params.get(key) + "  ");
            
            switch(key) {
                    
                case "-k":
                    keyToInsert = Integer.parseInt(params.get(key));
                    break;
                case "-p":
                    String tmp = params.get(key);
                    if(tmp.equals("r"))
                        typeOfPrinting = Statics.PRINT_RECORDS;
                    else if(tmp.equals("i"))
                        typeOfPrinting = Statics.PRINT_INDEX;
                    else if(tmp.equals("n"))
                        typeOfPrinting = Statics.NO_PRINT;
                    else
                        System.out.print("Incorrect param!");
                    break;
                default:
                    System.out.print("Incorrect param!");                  
            }
            
        }
        
        if(keyToInsert != -1 && (typeOfPrinting == Statics.PRINT_INDEX || 
                typeOfPrinting == Statics.PRINT_RECORDS ||
                typeOfPrinting == Statics.NO_PRINT)) {
            int ans = this.btree.insert(keyToInsert, 
                    this.random.getFloat(), this.random.getDouble(), typeOfPrinting);
            
            if(ans == Statics.ALREADY_EXISTS)
                System.out.println("Already exists!");
            else //KEY_INSERTED
                System.out.println("Key has been inserted");
        } else
            System.out.println("Too few params!");
    }
    
    private void executeUpdate(Map<String, String> params) throws IOException {
        int keyToUpdate = -1;
        int typeOfPrinting = 0;
        
        System.out.print("update ");
        
        for(String key : params.keySet()) {
            
            System.out.print(key + ": " + params.get(key) + "  ");
            
            switch(key) {
                    
                case "-k":
                    keyToUpdate = Integer.parseInt(params.get(key));
                    break;
                case "-p":
                    String tmp = params.get(key);
                    if(tmp.equals("r"))
                        typeOfPrinting = Statics.PRINT_RECORDS;
                    else if(tmp.equals("i"))
                        typeOfPrinting = Statics.PRINT_INDEX;
                    else if(tmp.equals("n"))
                        typeOfPrinting = Statics.NO_PRINT;
                    else
                        System.out.print("Incorrect param!");
                    break;
                default:
                    System.out.print("Incorrect param!");                  
            }
            
        }
        
        if(keyToUpdate != -1 && (typeOfPrinting == Statics.PRINT_INDEX || 
                typeOfPrinting == Statics.PRINT_RECORDS ||
                typeOfPrinting == Statics.NO_PRINT)) {
            int ans = this.btree.update(keyToUpdate, 
                    this.random.getFloat(), this.random.getDouble(), typeOfPrinting);
            
            if(ans == Statics.KEY_INSERTED)
                System.out.println("Key has been updated");
            else //KEY_INSERTED
                System.out.println("Key not found!");
        } else
            System.out.println("Too few params!");
    }
    
    private void executeRead(Map<String, String> params) throws IOException {
        int keyToRead = -1;
        
        System.out.print("read ");
        
        for(String key : params.keySet()) {
            
            System.out.print(key + ": " + params.get(key) + "  ");
            
            switch(key) {
                    
                case "-k":
                    String tmp = params.get(key);
                    if(tmp.equals("a"))
                        keyToRead = Statics.READ_ALL;
                    else
                        keyToRead = Integer.parseInt(tmp);
                    break;
                default:
                    System.out.print("Incorrect param!");                  
            }
        }
        
        if(keyToRead != -1) {
            int ans = this.btree.read(keyToRead); //ans -> pointer or NOT_FOUND
            
            if(ans != Statics.NOT_FOUND && ans != Statics.READ_ALL)
                System.out.println(this.btree.getRecords().getRecord(ans));
            else if(ans != Statics.READ_ALL)
                System.out.println("Key not found!");
        } else
            System.out.println("Too few params!");
    }
    
    private void executeRemove(Map<String, String> params) throws IOException {
        int keyToRemove = -1;
        int typeOfPrinting = 0;
        
        System.out.print("remove ");
        
        for(String key : params.keySet()) {
            
            System.out.print(key + ": " + params.get(key) + "  ");
            
            switch(key) {
                    
                case "-k":
                    keyToRemove = Integer.parseInt(params.get(key));
                    break;
                case "-p":
                    String tmp = params.get(key);
                    if(tmp.equals("r"))
                        typeOfPrinting = Statics.PRINT_RECORDS;
                    else if(tmp.equals("i"))
                        typeOfPrinting = Statics.PRINT_INDEX;
                    else if(tmp.equals("n"))
                        typeOfPrinting = Statics.NO_PRINT;
                    else
                        System.out.print("Incorrect param!");
                    break;
                default:
                    System.out.print("Incorrect param!");                  
            }
        }
        
        if(keyToRemove != -1 && (typeOfPrinting == Statics.PRINT_INDEX || 
                typeOfPrinting == Statics.PRINT_RECORDS ||
                typeOfPrinting == Statics.NO_PRINT)) {
            int ans = this.btree.remove(keyToRemove, typeOfPrinting); //ans -> pointer or NOT_FOUND
            
            if(ans != Statics.NOT_FOUND)
                System.out.println("Key has been removed");
            else
                System.out.println("Key not found!");
        } else
            System.out.println("Too few params!");
    }
    
    private boolean isBTreeInit() {
        if(this.btree != null) {
            return true;
        }
        
        System.out.println("Btree is not init!");
        return false;
    }
}

class ExitException extends Exception {
    
    public ExitException() {
        super();
    }
    
}

abstract class Statics {
    
    static public int NOT_FOUND = -1;
    static public int ALREADY_EXISTS = -2;
    static public int FOUND = -3;
    static public int STILL_FINDING = -4;
    static public int KEY_INSERTED = -5;
    static public int NULL_POINTER = -6;
    static public int LEFT = -7;
    static public int RIGHT = -8;
    static public int NOT_INSERTED = -9;
    static public int KEY_REMOVED = -10;
    static public int TO_INSERT = -11;
    static public int TO_REMOVE = -12;
    static public int PRINT_INDEX = -13;
    static public int PRINT_RECORDS = -14;
    static public int POINTER_ZERO = -15;
    static public int NO_PRINT = -16;
    static public int READ_ALL = -17;
    
}