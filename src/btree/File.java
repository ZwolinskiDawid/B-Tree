package btree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public abstract class File {
    
    protected RandomAccessFile file;
    protected RandomAccessFile recoveryFile;
    protected FileRecovery recovery;
    protected int pageSize;
    protected BTree btree;
    protected byte[] buffor;
    
    protected int counter;
    
    public File(String fileName, int d, BTree btree) throws FileNotFoundException, IOException {
        
        this.file = new RandomAccessFile(fileName, "rw");
        this.file.setLength(0);
        
        this.recoveryFile = new RandomAccessFile(fileName + "_recovery", "rw");
        this.recoveryFile.setLength(0);
        
        this.btree = btree;
        //obliczanie wielkości strony
        int sizeOfKeys = 2 * d * (4 + 4);
        int sizeOfPointers = (2 * d + 1) * 4;
        int sizeOfM = 4;
        this.pageSize = sizeOfKeys + sizeOfPointers + sizeOfM;
        this.buffor = new byte[this.pageSize];
        
        this.recovery = new FileRecovery(this.recoveryFile, pageSize);        
    }
    
    protected void addFreeSpace(int pointer) throws IOException {
        this.recovery.addFreeSpace(pointer);
    }
    
    public void remove(int pointer) throws IOException {
        this.addFreeSpace(pointer);
    }
    
    public void clearCounter() {
        this.counter = 0;
    }
    
    public int getCounter() {
        return this.counter;
    }
    
    protected int read(byte[] b, boolean save) throws IOException {
        if(save)
            this.counter++;
        return this.file.read(b);
    }
    
    protected void write(byte[] b, boolean save) throws IOException {
        if(save)
            this.counter++;
        this.file.write(b);
    }
    
}

class FileRecovery {
    
    private final RandomAccessFile file;
    private final int pageSize;
    private byte[] buffor;
    private int lastPageOffset;
    
    public FileRecovery(RandomAccessFile file, int pageSize) {
        this.file = file;
        //this.pageSize = pageSize;
        this.pageSize = 12;
        this.buffor = new byte[this.pageSize];
        
        this.lastPageOffset = this.pageSize / 4;
        this.lastPageOffset -= 1;
        this.lastPageOffset *= 4;
    }
    
    public int getFreeSpace() throws IOException {
        int freeSpacePointer = this.getfreeSpacePointer();
        
        if(freeSpacePointer == Statics.NULL_POINTER)
            return Statics.NULL_POINTER;
        
        int page = freeSpacePointer / this.pageSize;
        page *= this.pageSize;
        int offset = freeSpacePointer % this.pageSize;
        
        ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
        int pointer = wrapper.getInt(offset);
        if(pointer == Statics.POINTER_ZERO)
            pointer = 0;
        
        this.removePointer(page, offset);
        
        return pointer;
    }
    
    private int getfreeSpacePointer() throws IOException {
        
        if(this.file.length() == 0)
            return Statics.NULL_POINTER;
        
        int page = (int) (this.file.length() - this.pageSize);
        int offset = this.lastPageOffset;
        this.file.seek(page);
        this.file.read(this.buffor);
        ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
        
        while(offset >= 0) {
            if(wrapper.getInt(offset) != 0)
                return page + offset;
                
            offset -= 4;
        }
        
        return 1; //ten return nie powinien się nigdy wywołać
        
    }
    
    private void removePointer(int page, int offset) throws IOException {   
        if(offset == 0) {
            this.file.setLength(this.file.length() - this.pageSize);
        } else {
            ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
            wrapper.putInt(offset, 0);
            this.file.seek(page);
            this.file.write(this.buffor);
        }
    }
    
    public void addFreeSpace(int pointer) throws IOException {
        
        if(pointer == 0)
            pointer = Statics.POINTER_ZERO; //na wypadek wyszukiwania
        
        int lastFreeSpacePointer = this.getfreeSpacePointer();
        
        if(lastFreeSpacePointer == Statics.NULL_POINTER) {
            this.buffor = new byte[this.pageSize];
            ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
            wrapper.putInt(0, pointer);
            this.file.seek(0);
            this.file.write(this.buffor);
        } else {
            int page = lastFreeSpacePointer / this.pageSize;
            page *= this.pageSize;
            int offset = lastFreeSpacePointer % this.pageSize;
            offset += 4; //następne miejsce na pointer
            
            if(offset + 4 > this.pageSize) { //gdzie się kończy
                page = (int) this.file.length();
                this.buffor = new byte[this.pageSize];
                ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
                wrapper.putInt(0, pointer);
                this.file.seek(page);
                this.file.write(this.buffor);
            } else {
                ByteBuffer wrapper = ByteBuffer.wrap(this.buffor);
                wrapper.putInt(offset, pointer);
                this.file.seek(page);
                this.file.write(this.buffor);
            }
        }
    }
}
