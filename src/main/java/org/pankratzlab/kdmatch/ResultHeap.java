package org.pankratzlab.kdmatch;

//Sourced from https://robowiki.net/wiki/User:Chase-san/Kd-Tree . NB: ZLIB License

//"Everyone and their brother has one of these now, me and Simonton started it, but I was to
//inexperienced to get anything written, I took an hour or two to rewrite it today, because I am no
//longer completely terrible at these things. So here is mine if you care to see it.\n" +
//"\n" +
//"This and all my other code in which I display on the robowiki falls under the ZLIB License.\n" +
//"\n" +
//"Oh yeah, am I the only one that has a Range function?"
/**
* This is a KD Bucket Tree, for fast sorting and searching of K dimensional data.
* 
* @author Chase
* 
*/
public class ResultHeap<T> {
  private Object[] data;
  private double[] keys;
  private int capacity;
  private int size;

  protected ResultHeap(int capacity) {
    this.data = new Object[capacity];
    this.keys = new double[capacity];
    this.capacity = capacity;
    this.size = 0;
  }

  protected void offer(double key, T value) {
    int i = size;
    for (; i > 0 && keys[i - 1] > key; --i)
      ;
    if (i >= capacity) return;
    if (size < capacity) ++size;
    int j = i + 1;
    System.arraycopy(keys, i, keys, j, size - j);
    keys[i] = key;
    System.arraycopy(data, i, data, j, size - j);
    data[i] = value;
  }

  public double getMaxKey() {
    return keys[size - 1];
  }

  @SuppressWarnings("unchecked")
  public T removeMax() {
    if (isEmpty()) return null;
    return (T) data[--size];
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public boolean isFull() {
    return size == capacity;
  }

  public int size() {
    return size;
  }

  public int capacity() {
    return capacity;
  }
}