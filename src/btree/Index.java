package btree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class Index extends File {

    Index(String fileName, int d, BTree btree) throws FileNotFoundException, IOException {
         super(fileName, d, btree);
    }
    
    public IndexPage getRootPage() throws IOException {
        return this.getPage(0, null);
    }
    
    public IndexPage getPage(int pointer, IndexPage parent) throws IOException {
        
        if(pointer >= this.file.length() || pointer == Statics.NULL_POINTER)
            return null;
        
        this.file.seek(pointer);
        this.read(this.buffor, true);
        ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
        
        int m = wrapper.getInt();
        int p = wrapper.getInt(); //pierwszy pointer MOŻNA NA ŚLEPO WCZYTAĆ, STRONA NIGDY NIE BĘDZIE PUSTA
        IndexPage page = new IndexPage(this.btree, parent, pointer, m, 
                (p == 0) ? Statics.NULL_POINTER : p); //pierwszy pointer
        
        for(int i = 0; i < m; i++) {
            Key key = new Key(wrapper.getInt(), wrapper.getInt());
            page.getKeys().addLast(key);
            
            p = wrapper.getInt();
            page.getPointers().addLast((p == 0) ? Statics.NULL_POINTER : p);
        }

        return page;
        
    }
    
    public IndexPage createRootPage() throws IOException {
        return this.createPage(null);
    }
    
    //tworzenie strony w pliku i zwracanie do niej wskaźnika
    public IndexPage createPage(IndexPage parent) throws IOException {
        int pointer = this.recovery.getFreeSpace();
        if(pointer == Statics.NULL_POINTER)
            pointer = (int) this.file.length();
        
        this.buffor = new byte[this.pageSize];
        ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
        
        wrapper.putInt(0); //m
        wrapper.putInt(Statics.NULL_POINTER); //firstpointer
        this.file.seek(pointer);
        this.write(this.buffor, true);
        
        return new IndexPage(this.btree, parent, pointer, 0, Statics.NULL_POINTER);        
    }
    
    public void savePage(IndexPage page) throws IOException {
        this.buffor = new byte[this.pageSize];
        ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
        
        LinkedList<Integer> pointers = (LinkedList<Integer>) page.getPointers().clone();
        LinkedList<Key> keys = (LinkedList<Key>) page.getKeys().clone();
        
        wrapper.putInt(page.getM()); //m
        wrapper.putInt(pointers.removeFirst()); //first pointer
        
        for(int i = 0; i < page.getM(); i++) {
            Key key = keys.removeFirst();
            wrapper.putInt(key.getValue());
            wrapper.putInt(key.getRecordPointer());
            wrapper.putInt(pointers.removeFirst());
        }
        
        this.file.seek(page.getPagePointer());
        this.write(this.buffor, true);
    }
    
    public void movePage(Integer pointerFrom, Integer pointerTo) throws IOException {
        
        this.remove(pointerFrom);
        
        this.file.seek(pointerFrom);
        this.read(this.buffor, true);
        this.file.seek(pointerTo);
        this.write(this.buffor, true);
        
    }
    
}
