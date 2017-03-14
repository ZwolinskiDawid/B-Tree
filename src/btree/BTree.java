package btree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;

public class BTree {

    private Index index;
    private Records records;
    LinkedList<IndexPage> listOfPages;
    private int d;
    
    private PrintStream ps;
    
    BTree(String index, String records, int d, String fileName) throws FileNotFoundException, IOException {
        
        this.index = new Index(index, d, this);
        this.records = new Records(records, d, this);
        this.listOfPages = new LinkedList();
        this.d = d;
        
        this.ps = new PrintStream(new java.io.File(fileName));
        
    }
    
    public FindResult find(IndexPage page, int keyToFind) throws IOException {
        
        if(page == null)
            return new FindResult(Statics.NOT_FOUND);
        else {
            this.listOfPages.add(page);
            FindResult result = page.findKey(keyToFind);
            
            if(result.getType() == Statics.FOUND)
                return result;
            else //still finding    result.getResult() to index dziecka, w którym będziemy szukać
                return this.find(this.index.getPage(page.getChildPointer(result.getResult()), page), 
                                                        keyToFind);
        }
    }
    
    public int insert(int keyToInsert, float velocity, double mass, int typeOfPrinting) throws IOException {
        
        this.index.clearCounter();
        this.records.clearCounter();        
        
        FindResult result = this.find(this.index.getRootPage(), keyToInsert);
        
        try {
            if(result.getType() == Statics.FOUND)
                return Statics.ALREADY_EXISTS;
            
            //tworzenie nowego klucza
            Key key = new Key(keyToInsert, 
                    this.getRecords().insert(new Record(velocity, mass)));
            Integer pointer = Statics.NULL_POINTER;
            
            //sytuacja kiedy nie ma nawet korzenia
            if(this.listOfPages.isEmpty()) { 
                IndexPage page = this.index.createRootPage();
                return page.insertKey(key, pointer, true); //na poczatek
            }
            
            return this.insertRecurs(key, pointer);
            
        } finally {
            this.listOfPages.clear();
            this.print(typeOfPrinting);
            this.printCounters();
        }
    }
    
    private int insertRecurs(Key keyToInsert, Integer pointer) throws IOException {

        IndexPage page = this.listOfPages.removeLast();
        if (!page.isFull())
            return page.insertKey(keyToInsert, pointer, true);

        //spróbuj kompensacji
        PageToCompensation pageToComp = page.getPageToCompensation(Statics.TO_INSERT);
        if(pageToComp != null) {
            page.insertKey(keyToInsert, pointer, false); //force insert

            if(pageToComp.getType() == Statics.RIGHT)
                this.compensation(page, pageToComp.getPage());
            else
                this.compensation(pageToComp.getPage(), page);

            return Statics.KEY_INSERTED;
        }

        //rozszczepianie
        page.insertKey(keyToInsert, pointer, false); //force insert
        return this.split(page);

        //return Statics.NOT_INSERTED; // ten return nie powinien się wywołać;
    }
    
    private void compensation(IndexPage leftPage, IndexPage rightPage) throws IOException {
        
        IndexPage parent = leftPage.getParent(); //dzieci zawsze mają rodzica
        int indexAsChild = leftPage.getIndexAsChild();
        RemovedElement removedElement = null; //zawsze będzie inicjalizowana
        
        //lewa strona jest przepełniona
        if(leftPage.getM() > rightPage.getM()) {
            int pagesToMove = (leftPage.getM() - rightPage.getM()) / 2;
            
            for(int i = 0; i < pagesToMove; i++) {
                //usuń ostatni klucz wraz z prawym wskaźnikiem z lewego brata
                removedElement = leftPage.removeLast();
                //dodaj jako pierwszy klucz klucz rodzica oraz jako pierwszy usunięty wskaźnik
                rightPage.insertKeyFirst(parent.getKeys().get(indexAsChild), 
                        removedElement.getPointer());
                
                //ustaw usunięty klucz w rodzicu
                parent.getKeys().set(indexAsChild, removedElement.getKey());
            }
        }
        
        //prawa jest przepełniona
        else {
            int pagesToMove = (rightPage.getM() - leftPage.getM()) / 2;
            
            for(int i = 0; i < pagesToMove; i++) {
                //usuń ostatni klucz wraz z prawym wskaźnikiem z lewego brata
                removedElement = rightPage.removeFirst();
                //dodaj jako pierwszy klucz klucz rodzica oraz jako pierwszy usunięty wskaźnik
                leftPage.insertKeyLast(parent.getKeys().get(indexAsChild), 
                        removedElement.getPointer());
                
                //ustaw usunięty klucz w rodzicu
                parent.getKeys().set(indexAsChild, removedElement.getKey());
            }
        }
        
        this.index.savePage(leftPage);
        this.index.savePage(rightPage);
        this.index.savePage(parent);
    }
    
    private int split(IndexPage rightPage) throws IOException {
        
        IndexPage parent = null;

        //strona jest rootem, trzeba stworzyć nowego roota
        if(this.listOfPages.isEmpty()) {
            parent = this.index.createPage(null);
            rightPage.setParent(parent);
            
            //zamiana korzeni
            rightPage.setPagePointer(parent.getPagePointer());
            parent.setPagePointer(0);
            parent.getPointers().set(0, rightPage.getPagePointer());
            this.index.savePage(parent);            
            //==============            
            
            this.listOfPages.add(parent);
            
        } else { //strona nie jest rootem
            parent = this.listOfPages.getLast(); //dzieci zawsze mają rodzica
            //nie można wykasować, będzie potrzebne przy rekurencji
        }
        
        IndexPage leftPage = this.index.createPage(parent);
        //przeniesienie ostatniego wskaźnika
        leftPage.getPointers().set(0, rightPage.getPointers().removeFirst());
        
        int pagesToMove = (rightPage.getM() - 1) / 2;
        
        for(int i = 0; i < pagesToMove; i++) {
            RemovedElement removedElement = rightPage.removeFirst();
            
            leftPage.insertKeyLast(removedElement.getKey(), 
                    removedElement.getPointer());
        }
        
        //wyrzucanie środkowego klucza do góry
        Key key = rightPage.getKeys().removeFirst();
        rightPage.setM(rightPage.getM() - 1); // bo pobraliśmy klucz
        Integer pointer = leftPage.getPagePointer();
        
        this.index.savePage(leftPage);
        this.index.savePage(rightPage);
        
        return this.insertRecurs(key, pointer);
    }
    
    public void print(int typeOfPrinting) throws IOException {
        System.out.println();
        
        if(typeOfPrinting == Statics.PRINT_INDEX)
            this.index.getRootPage().print(0);
        else if(typeOfPrinting == Statics.PRINT_RECORDS)
            this.records.print();
    }
    
    public int read(int keyToRead) throws IOException {
        
        if(keyToRead == Statics.READ_ALL)
            return this.printByKey();          
        
        FindResult result = this.find(this.index.getRootPage(), keyToRead);
        
        if(result.getType() == Statics.FOUND)
            return result.getResult();
        else
            return Statics.NOT_FOUND;
    }
    
    public int update(int keyToUpdate, float velocity, double mass, int typeOfPrinting) throws IOException {     
        
        this.index.clearCounter();
        this.records.clearCounter();
        
        FindResult result = this.find(this.index.getRootPage(), keyToUpdate);
        
        try {
            if(result.getType() == Statics.NOT_FOUND) //uwzględnia sytuację kiedy drzewo jest puste
                return Statics.NOT_FOUND;
            
            IndexPage page = this.listOfPages.getLast();
            page.updateKey(keyToUpdate, velocity, mass);
            
            return Statics.KEY_INSERTED;
            
        } finally {
            this.listOfPages.clear();
            this.print(typeOfPrinting);
            this.printCounters();
        }
    }
    
    private int printByKey() throws IOException {
        System.out.println();
        this.index.getRootPage().printByKey();
        return Statics.READ_ALL;
    }
    
    public int remove(int keyToRemove, int typeOfPrinting) throws IOException {
        
        this.index.clearCounter();
        this.records.clearCounter();
        
        FindResult result = this.find(this.index.getRootPage(), keyToRemove);
        
        try {
            if(result.getType() == Statics.NOT_FOUND) //uwzględnia sytuację kiedy drzewo jest puste
                return Statics.NOT_FOUND;
            
            IndexPage page = this.listOfPages.getLast();
            
            if(page.isLeaf()) {
                
                page.RemoveKey(keyToRemove);
                return this.repair(page);
                
            }
            
            KeyToReplace keyToReplace = this.getKeyToReplace(keyToRemove); //z poddrzewa
            
            //odzyskiwanie miejsca w records
            Key removingKey = page.getKeys().get(keyToReplace.getIndexOfKey());
            this.records.remove(removingKey.getRecordPointer());
            //==============================
            
            page.getKeys().set(keyToReplace.getIndexOfKey(), 
                    keyToReplace.getKey());
            this.index.savePage(page);
            
            return this.repair(keyToReplace.getLeaf());
            
        } finally {
            this.listOfPages.clear();
            this.print(typeOfPrinting);
            this.printCounters();
        }
        
    }
    
    private KeyToReplace getKeyToReplace(int keyToRemove) throws IOException {
        
        //jeśli z żadnego liścia nie można brać to bierz na siłę z lewego
        //i jednocześnie go usuwaj z liścia
        //dodawanie do listOfPages kolejnych stron
        
        IndexPage page = this.listOfPages.removeLast();
        int index = page.getIndexOfKey(keyToRemove);
        
        IndexPage leftPage = this.index.getPage(page.getPointers().get(index), page);
        IndexPage leftLeaf = this.getLeafFromSubTree(leftPage, Statics.RIGHT); //lewe poddrzewo
        
        IndexPage rightLeaf = null;
        if(leftLeaf.getM() == this.d) {
            IndexPage rightPage = this.index.getPage(page.getPointers().get(index + 1), page);
            rightLeaf = this.getLeafFromSubTree(rightPage, Statics.LEFT);
        }
        
        if(rightLeaf == null || rightLeaf.getM() == this.d) {
            RemovedElement removedElement = leftLeaf.removeLast();
            this.index.savePage(leftLeaf);
            return new KeyToReplace(removedElement.getKey(), index, leftLeaf);
        } else {
            RemovedElement removedElement = rightLeaf.removeFirst();
            this.index.savePage(rightLeaf);
            return new KeyToReplace(removedElement.getKey(), index, rightLeaf);
        }
    }
    
    private IndexPage getLeafFromSubTree(IndexPage page, int type) throws IOException {
        
        Integer childPointer;
        
        if(type == Statics.RIGHT) //lewe poddrzewo
            childPointer = page.getPointers().getLast();
        else
            childPointer = page.getPointers().getFirst();
        
        IndexPage child = this.index.getPage(childPointer, page);
        
        if(child != null)
            return this.getLeafFromSubTree(child, type);
        else
            return page;
    }
    
    private int repair(IndexPage page) throws IOException {
        
        if(page.getPagePointer().equals(0)) { //jest rootem
            
            if(page.getM() < 1)
                this.index.movePage(page.getPointers().removeLast(), 0);
            
            return Statics.KEY_REMOVED;
            
        } else {
            
            if(page.getM() >= this.getD())
                return Statics.KEY_REMOVED;

            //spróbuj kompensacji
            PageToCompensation pageToComp = page.getPageToCompensation(Statics.TO_REMOVE);
            if(pageToComp != null) {
                if(pageToComp.getType() == Statics.RIGHT)
                    this.compensation(page, pageToComp.getPage());
                else
                    this.compensation(pageToComp.getPage(), page);

                return Statics.KEY_REMOVED;
            }
            
            //scalanie
            int index = page.getIndexAsChild();
            if(index > 0) {
                Integer leftPagePointer = page.getParent().getPointers().get(index - 1);
                IndexPage leftPage = this.index.getPage(leftPagePointer, page.getParent());
                return this.merge(leftPage, page);
            } else {
                Integer rightPagePointer = page.getParent().getPointers().get(index + 1);
                IndexPage rightPage = this.index.getPage(rightPagePointer, page.getParent());
                return this.merge(page, rightPage);
            }
        }
    }
    
    private int merge(IndexPage leftPage, IndexPage rightPage) throws IOException {
        
        int index = leftPage.getIndexAsChild();
        IndexPage parent = leftPage.getParent();
        Key key = parent.removeKey(index);
        this.index.remove(leftPage.getPagePointer()); //zwalnianie miejsca po lewej stronie
        
        rightPage.insertKeyFirst(key, leftPage.getPointers().removeLast());
        
        int pagesToMove = leftPage.getM();
        
        for(int i = 0; i < pagesToMove; i++) {
            RemovedElement removedElement = leftPage.removeLast();
            
            rightPage.insertKeyFirst(removedElement.getKey(), 
                    removedElement.getPointer());
        }
        
        this.index.savePage(rightPage);
        return this.repair(parent);
    }
    
    public Index getIndex() {
        return this.index;
    }
    
    public Records getRecords() {
        return this.records;
    }
    
    public int getD() {
        return this.d;
    }
    
    private void printCounters() {
        this.ps.println(this.index.getCounter() + this.records.getCounter());
    }
    
    public void printFileSizes() throws IOException {
        this.ps.println(this.index.file.length());
        this.ps.println(this.records.file.length());
    }
    
}

class FindResult {
    
    final private int type;
    final private int result;
    
    public FindResult(int type, int result) {
        this.type = type;
        this.result = result;
    }
    
    public FindResult(int type) {
        this.type = type;
        this.result = Statics.NULL_POINTER;
    }
    
    public int getType() {
        return this.type;
    }
    
    public int getResult() {
        return this.result;
    }
    
}

class PageToCompensation {
    
    private final IndexPage page;
    private final int type;
    
    public PageToCompensation(IndexPage page, int type) {
        this.page = page;
        this.type = type;
    }
    
    public IndexPage getPage() {
        return this.page;
    }
    
    public int getType() {
        return this.type;
    }
    
}

class KeyToReplace {
    
    private final Key key;
    private final int indexOfKey; //index zamienianego klucza
    private final IndexPage leaf;
    
    public KeyToReplace(Key key, int indexOfKey, IndexPage leaf) {
        this.key = key;
        this.indexOfKey = indexOfKey;
        this.leaf = leaf;
    }
    
    public Key getKey() {
        return this.key;
    }
    
    public int getIndexOfKey() {
        return this.indexOfKey;
    }
    
    public IndexPage getLeaf() {
        return this.leaf;
    }
}