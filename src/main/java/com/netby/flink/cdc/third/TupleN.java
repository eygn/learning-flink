package com.netby.flink.cdc.third;

/**
 * @author: byg
 * @date: 2023/5/20 16:45
 */
public class TupleN extends BaseTuple {
    private static final long serialVersionUID = -2585891178772468170L;
    private int size;
    private Object[] fields;

    public TupleN(int size) {
        this.setSize(size);
    }

    public <T> T getField(int pos) {
        return (T) this.fields[pos];
    }

    public <T> void setField(int pos, T value) {
        this.fields[pos] = value;
    }

    public int size() {
        return this.size;
    }

    public TupleN copy() {
        TupleN tuple = new TupleN(this.size);
        System.arraycopy(this.fields, 0, tuple.fields, 0, this.size);
        return tuple;
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
        if (this.fields == null) {
            this.fields = new Object[size];
        }

    }

    public TupleN() {
    }

}
