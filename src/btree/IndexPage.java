package btree;

import java.io.IOException;
import java.util.LinkedList;

public class IndexPage {
    
    private final BTree btree;
    private IndexPage parent;
    private int pagePointer;
    
    //dane do serializacji
    private int m;
    private final LinkedList<Integer> pointers;
    private final LinkedList<Key> keys;
    
    public IndexPage(BTree btree, IndexPage parent, int pagePointer, int m, int firstPointer) {
        
        //wpisać klucze i wskaźniki do list
        
        this.btree = btree;
        this.parent = parent; //może być null - dalej w kodzie będzie poprawnie zmieniony jeśli nie jest rootem (rozszczepianie)
        this.pagePointer = pagePointer;
        this.m = m;
        this.pointers = new LinkedList();
        this.pointers.add(firstPointer);
        this.keys = new LinkedList();
        
    }
    
    public FindResult findKey(int keyToFind) {
        
        int index = 0;
        
        for(Key key : this.keys) {
            
            if(key.getValue() == keyToFind)
                return new FindResult(Statics.FOUND, key.getRecordPointer());
            else if(key.getValue() > keyToFind)
                return new FindResult(Statics.STILL_FINDING, index);
                
            index++;
        }
        
        return new FindResult(Statics.STILL_FINDING, index);
        
    }
    
    public int getIndexOfKey(int keyToFind) {
        int index = 0;
        
        for(Key key : this.keys) {
            if(key.getValue() >= keyToFind)
                return index;
                
            index++;
        }
        
        return index;
    }
    
    public int insertKey(Key keyToInsert, Integer pointer, boolean saveAfter) throws IOException {
        return this.insertKey(this.getIndexOfKey(keyToInsert.getValue()), 
                keyToInsert, pointer, saveAfter);
    }
    
    private int insertKey(int index, Key key, Integer pointer, boolean saveAfter) throws IOException {
        //this.keys.add(index, new Key(keyToInsert, this.btree.getRecords().insert()));
        this.keys.add(index, key);
        this.pointers.add(index, pointer);
        this.m++;
        //zapisz
        if(saveAfter)
            this.btree.getIndex().savePage(this);
        
        return Statics.KEY_INSERTED;
    }
    
    public void insertKeyFirst(Key key, Integer pointer) {
        this.keys.addFirst(key);
        this.pointers.addFirst(pointer);
        this.m++;
    }
    
    public void insertKeyLast(Key key, Integer pointer) {
        this.keys.addLast(key);
        this.pointers.addLast(pointer);
        this.m++;
    }
    
    public RemovedElement removeLast() {
        this.m--;
        return new RemovedElement(this.keys.removeLast(), 
                this.pointers.removeLast());
    }
    
    public RemovedElement removeFirst() {
        this.m--;
        return new RemovedElement(this.keys.removeFirst(), 
                this.pointers.removeFirst());
    }
    
    public void RemoveKey(int keyToRemove) throws IOException { //tylko z liścia
        int index = this.getIndexOfKey(keyToRemove);
        
        //odzyskiwanie miejsca w records
        Key removingKey = this.keys.get(index);
        this.btree.getRecords().remove(removingKey.getRecordPointer());
        //==============================
        
        this.keys.remove(index);
        this.pointers.remove(index);
        this.m--;
        
        this.btree.getIndex().savePage(this);
    }
    
    public void updateKey(int keyToUpdate, float velocity, double mass) throws IOException {
        int index = this.getIndexOfKey(keyToUpdate);
        int pointer = this.keys.get(index).getRecordPointer();
        this.btree.getRecords().update(pointer, new Record(velocity, mass));
    }
    
    public Key removeKey(int index) throws IOException {
        
        Key key = this.keys.remove(index);
        this.pointers.remove(index);
        this.m--;
        
        this.btree.getIndex().savePage(this);
        
        return key;
    }
    
    public void setParent(IndexPage parent) {
        this.parent = parent;
    }
    
    public Integer getPagePointer() {
        return this.pagePointer;
    }
    
    public void setPagePointer(Integer pointer) {
        this.pagePointer = pointer;
    }
    
    public void setM(int m) {
        this.m = m;
    }
    
    public PageToCompensation getPageToCompensation(int type) throws IOException {
        
        if(this.parent == null) 
            return null;
        
        int index = this.getIndexAsChild();
        
        if(index > 0) { //spróbuj lewego brata
            int leftPagePointer = this.parent.getPointers().get(index - 1);
            IndexPage leftPage = this.btree.getIndex().getPage(leftPagePointer, this.parent);
            
            if(leftPage != null) {
                
                if(type == Statics.TO_INSERT && !leftPage.isFull())
                    return new PageToCompensation(leftPage, Statics.LEFT);
                else if(type == Statics.TO_REMOVE && leftPage.getM() > this.btree.getD())
                    return new PageToCompensation(leftPage, Statics.LEFT);
                
            }
        }
        
        //spróbuj prawego brata
        if(index < this.parent.getM()) {
            int rightPagePointer = this.parent.getPointers().get(index + 1);
            IndexPage rightPage = this.btree.getIndex().getPage(rightPagePointer, this.parent);
            
            if(rightPage != null) {
                
                if(type == Statics.TO_INSERT && !rightPage.isFull())
                    return new PageToCompensation(rightPage, Statics.RIGHT);
                else if(type == Statics.TO_REMOVE && rightPage.getM() > this.btree.getD())
                    return new PageToCompensation(rightPage, Statics.RIGHT);
                
            }
        }
        
        return null;
    }
    
    public int getIndexAsChild() {
        int index = 0;
        for(Integer pointer : this.parent.pointers) {
            if(pointer.equals(this.pagePointer))
                return index;
            
            index++;
        }
        
        return Statics.NULL_POINTER; // program nigdy nie powinien tego zwrócić
    }
    
    public int getChildPointer(int index) {
        return this.pointers.get(index);
    }
    
    public boolean isFull() {
        return this.m >= 2 * this.btree.getD();
    }
    
    public int getM() {
        return this.m;
    }
    
    public IndexPage getParent() {
        return this.parent;
    }
    
    public boolean isLeaf() {
        return this.pointers.getFirst() == Statics.NULL_POINTER;
    }
    
    public LinkedList<Key> getKeys() {
        return this.keys;
    }
    
    public LinkedList<Integer> getPointers() {
        return this.pointers;
    }
    
    public void print(int pad) throws IOException {
        this.printPad(pad);
        
        for(Key key : this.keys) {
            System.out.print(key.getValue() + " ");
        }
        System.out.println("");
        
        for(Integer pointer : this.pointers) {
            if(pointer != Statics.NULL_POINTER)
                this.btree.getIndex().getPage(pointer, this).print(pad + 1);
        }
    }
    
    public void printByKey() throws IOException {
        int index = 0, pointer = 0;
        for(Key key : this.keys) {
            pointer = this.pointers.get(index);
            IndexPage child = this.btree.getIndex().getPage(pointer, this);
            if(child != null)
                child.printByKey();
            System.out.println(key.getValue() + " " + 
                    this.btree.getRecords().getRecord(key.getRecordPointer()));
            index++;
        }
        pointer = this.pointers.get(index);
        IndexPage child = this.btree.getIndex().getPage(pointer, this);
        if(child != null)
            child.printByKey();
    }
    
    private void printPad(int pad) {
        for(int i = 0; i < pad; i++)
            System.out.print("-");
    }
    
}

class Key {
    
    private final int value;
    private final int recordPointer;
    
    public Key(int value, int pointer) {
        this.value = value;
        this.recordPointer = pointer;
    }
    
    public int getValue() {
        return this.value;
    }
    
    public int getRecordPointer() {
        return this.recordPointer;
    }
    
}

class RemovedElement {
    
    private final Key key;
    private final Integer pointer;
    
    public RemovedElement(Key key, Integer pointer) {
        this.key = key;
        this.pointer = pointer;
    }
    
    public Key getKey() {
        return this.key;
    }
    
    public Integer getPointer() {
        return this.pointer;
    }
    
}