package io.teknek.cassandra.simple;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompositeTool { 
  public static final int NON_INCLUSIVE_START = -1;
  public static final int INCLUSIVE_END = 1;
  public static final int NORMAL = 0;
    
  public static List<byte[]> bbArrayToByteArray(List<ByteBuffer> b) {
    List<byte[]> b1 = new ArrayList<byte[]>();
    for (ByteBuffer bb : b) {
      b1.add(ByteBufferUtil.getArray(bb));
    }
    return b1;
  }
  
  public static List<byte[]> stringArrayToByteArray(List<String> strings){
    List<byte[]> convert = new ArrayList<byte[]>(strings.size());
    for (String s: strings){
      try {
        convert.add(s.getBytes("UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    return convert;
  }

  public static List<ByteBuffer> byteArrayToBBArray(List<byte[]> b) {
    List<ByteBuffer> b1 = new ArrayList<ByteBuffer>();
    for (byte[] bb : b) {
      b1.add(ByteBuffer.wrap(bb));
    }
    return b1;
  }

  /**
   * Build a composite column using the specified separators
   * @param data
   * @param separator
   * @return a composite column based on input
   */
  public static byte[] makeComposite(List<byte[]> data, int[] separator) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < data.size(); i++) {
      bos.write((byte) ((data.get(i).length >> 8) & 0xFF));
      bos.write((byte) (data.get(i).length & 0xFF));
      for (int j = 0; j < data.get(i).length; j++) {
        bos.write(data.get(i)[j] & 0xFF);
      }
      bos.write((byte) (separator[i] & 0xFF));
    }
    return bos.toByteArray();
  }

  /**
   * a composite column based on default separators
   * @param b
   * @return
   */
  public static byte[] makeComposite(List<byte[]> b) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < b.size(); i++) {
      bos.write((byte) ((b.get(i).length >> 8) & 0xFF));
      bos.write((byte) (b.get(i).length & 0xFF));
      for (int j = 0; j < b.get(i).length; j++) {
        bos.write(b.get(i)[j] & 0xFF);
      }
      bos.write((byte) 0);
    }
    return bos.toByteArray();
  }

  public static List<byte[]> readComposite(byte[] column) {
    List<byte[]> result = new ArrayList<byte[]>();
    for (int i = 0; i < column.length; i++) {
      int length = (column[i++] & 0xFF) << 8;
      length = (column[i++] & 0xFF);
      byte[] data = new byte[length];
      for (int j = 0; j < length; j++) {
        data[j] = column[i++];
      }
      result.add(data);
    }
    return result;
  }
  
  public static void prettyPrintComposite(byte [] column, List<AbstractType> columnType){
    List<byte[]> parts = readComposite(column);
    for (int i =0;i<parts.size();i++){
      try {
        System.out.println( columnType.get(i).getString(ByteBuffer.wrap(parts.get(i))) );
      } catch (Exception ex){}
    }
  }
}
