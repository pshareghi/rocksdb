// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;
import java.util.ArrayList;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

 /* @Test
  public void stringOption()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOperatorName("stringappend");

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }*/
  
  @Test
  public void mergeOperator()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOperatorName("stringappend");

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.merge("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void mergeOperator3()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOperatorName("stringappend");

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.merge("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      // merge bb under key
      db.merge("key".getBytes(), "cc".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb,cc");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void addMergeOpr2()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new LongAddMergeOpr(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing max int value under key
      db.put("key".getBytes(), LongAddMergeOpr.encodeLong(2147483647));
      // merge 1 under key
      db.merge("key".getBytes(), LongAddMergeOpr.encodeLong(1));

      byte[] value = db.get("key".getBytes());
      long longValue = LongAddMergeOpr.decodeLong(value);
      assertThat(longValue).isEqualTo(2147483648L);
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  } 
  
  @Test
  public void addMergeOpr3()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new LongAddMergeOpr(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing max int value under key
      db.merge("key".getBytes(), LongAddMergeOpr.encodeLong(2147483647));
      
      // merge 1 under key
      db.merge("key".getBytes(), LongAddMergeOpr.encodeLong(1));

   // merge 1 under key
      db.merge("key".getBytes(), LongAddMergeOpr.encodeLong(1));

      byte[] value = db.get("key".getBytes());

      long longValue = LongAddMergeOpr.decodeLong(value);
      assertThat(longValue).isEqualTo(2147483649L);
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  } 
  
  
  public static class AppendMerge extends AssociativeMergeOpr {
    public AppendMerge(final MergeOprOptions mopt) {
      super(mopt);
    }
    
    public String name() {
      return this.getClass().getName();
    }
    
    public byte[] merge(Slice key,
        Slice existingValue,
        Slice value) {
      
      String str1 = "";
      if (existingValue.isInitialized()) {
        str1 = new String(existingValue.data());
      }
      
      String str2 = new String(value.data());
      String merged = str1 + ":" + str2;
      return merged.getBytes();
    }
  }
  
  @Test
  public void mergeOpr2()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new AppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void mergeOpr3()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new AppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void mergeOpr4()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new AppendMerge(new MergeOprOptions()));
      
      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());
      // merge dd under key
      db.merge("key".getBytes(), "dd".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc:dd");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void mergeOpr5()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new AppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());
      // merge dd under key
      db.merge("key".getBytes(), "dd".getBytes());
      // merge ee under key
      db.merge("key".getBytes(), "ee".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc:dd:ee");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  public static class DirectAppendMerge extends DirectAssociativeMergeOpr {
    public DirectAppendMerge(final MergeOprOptions mopt) {
      super(mopt);
    }
    
    public String name() {
      return this.getClass().getName();
    }
    
    public byte[] merge(DirectSlice key,
        DirectSlice existingValue,
        DirectSlice value) {
      
      byte[] existingBytes = new byte[existingValue.data().remaining()];
      byte[] valueBytes = new byte[value.data().remaining()];
      
      existingValue.data().get(existingBytes);
      value.data().get(valueBytes);
      
      String existingStr = new String(existingBytes);
      String valueStr = new String(valueBytes);
      
      String mergedStr = existingStr + ":" + valueStr;
      return mergedStr.getBytes();
    }
  }
  
  @Test
  public void directMrgeOpr2()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new DirectAppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void directMergeOpr3()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new DirectAppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void directMergeOpr4()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new DirectAppendMerge(new MergeOprOptions()));
      
      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());
      // merge dd under key
      db.merge("key".getBytes(), "dd".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc:dd");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
  @Test
  public void directMergeOpr5()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      opt.setMergeOpr(new DirectAppendMerge(new MergeOprOptions()));

      db = RocksDB.open(opt, db_path_string);
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());
      // merge cc under key
      db.merge("key".getBytes(), "cc".getBytes());
      // merge dd under key
      db.merge("key".getBytes(), "dd".getBytes());
      // merge ee under key
      db.merge("key".getBytes(), "ee".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa:bb:cc:dd:ee");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }
  
/*
  @Test
  public void cFStringOption()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    DBOptions opt = null;
    List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new DBOptions();
      opt.setCreateIfMissing(true);
      opt.setCreateMissingColumnFamilies(true);

      List<ColumnFamilyDescriptor> cfDescriptors =
          new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
          new ColumnFamilyOptions().setMergeOperatorName(
              "stringappend")));
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
          new ColumnFamilyOptions().setMergeOperatorName(
              "stringappend")));
      db = RocksDB.open(opt, db_path_string,
          cfDescriptors, columnFamilyHandleList);

      // writing aa under key
      db.put(columnFamilyHandleList.get(1),
          "cfkey".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge(columnFamilyHandleList.get(1),
          "cfkey".getBytes(), "bb".getBytes());

      byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
      String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb");
    } finally {
      for (ColumnFamilyHandle handle : columnFamilyHandleList) {
        handle.dispose();
      }
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test
  public void operatorOption()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    Options opt = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);

      StringAppendOperator stringAppendOperator = new StringAppendOperator();
      opt.setMergeOperator(stringAppendOperator);

      db = RocksDB.open(opt, db_path_string);
      // Writing aa under key
      db.put("key".getBytes(), "aa".getBytes());

      // Writing bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      byte[] value = db.get("key".getBytes());
      String strValue = new String(value);

      assertThat(strValue).isEqualTo("aa,bb");
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test
  public void cFOperatorOption()
      throws InterruptedException, RocksDBException {
    RocksDB db = null;
    DBOptions opt = null;
    ColumnFamilyHandle columnFamilyHandle = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new DBOptions();
      opt.setCreateIfMissing(true);
      opt.setCreateMissingColumnFamilies(true);
      StringAppendOperator stringAppendOperator = new StringAppendOperator();

      List<ColumnFamilyDescriptor> cfDescriptors =
          new ArrayList<>();
      List<ColumnFamilyHandle> columnFamilyHandleList =
          new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
          new ColumnFamilyOptions().setMergeOperator(
              stringAppendOperator)));
      cfDescriptors.add(new ColumnFamilyDescriptor("new_cf".getBytes(),
          new ColumnFamilyOptions().setMergeOperator(
              stringAppendOperator)));
      db = RocksDB.open(opt, db_path_string,
          cfDescriptors, columnFamilyHandleList);

      // writing aa under key
      db.put(columnFamilyHandleList.get(1),
          "cfkey".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge(columnFamilyHandleList.get(1),
          "cfkey".getBytes(), "bb".getBytes());
      byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
      String strValue = new String(value);

      // Test also with createColumnFamily
      columnFamilyHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf2".getBytes(),
              new ColumnFamilyOptions().setMergeOperator(stringAppendOperator)));
      // writing xx under cfkey2
      db.put(columnFamilyHandle, "cfkey2".getBytes(), "xx".getBytes());
      // merge yy under cfkey2
      db.merge(columnFamilyHandle, new WriteOptions(), "cfkey2".getBytes(), "yy".getBytes());
      value = db.get(columnFamilyHandle, "cfkey2".getBytes());
      String strValueTmpCf = new String(value);

      columnFamilyHandle.dispose();
      assertThat(strValue).isEqualTo("aa,bb");
      assertThat(strValueTmpCf).isEqualTo("xx,yy");
    } finally {
      if (columnFamilyHandle != null) {
        columnFamilyHandle.dispose();
      }
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test
  public void operatorGcBehaviour()
      throws RocksDBException {
    Options opt = null;
    RocksDB db = null;
    try {
      String db_path_string =
          dbFolder.getRoot().getAbsolutePath();
      opt = new Options();
      opt.setCreateIfMissing(true);
      StringAppendOperator stringAppendOperator = new StringAppendOperator();
      opt.setMergeOperator(stringAppendOperator);
      db = RocksDB.open(opt, db_path_string);
      db.close();
      opt.dispose();
      System.gc();
      System.runFinalization();
      // test reuse
      opt = new Options();
      opt.setMergeOperator(stringAppendOperator);
      db = RocksDB.open(opt, db_path_string);
      db.close();
      opt.dispose();
      System.gc();
      System.runFinalization();
      // test param init
      opt = new Options();
      opt.setMergeOperator(new StringAppendOperator());
      db = RocksDB.open(opt, db_path_string);
      db.close();
      opt.dispose();
      System.gc();
      System.runFinalization();
      // test replace one with another merge operator instance
      opt = new Options();
      opt.setMergeOperator(stringAppendOperator);
      StringAppendOperator newStringAppendOperator = new StringAppendOperator();
      opt.setMergeOperator(newStringAppendOperator);
      db = RocksDB.open(opt, db_path_string);
      db.close();
      opt.dispose();
    } finally {
      if (db != null) {
        db.close();
      }
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test
  public void emptyStringInSetMergeOperatorByName() {
    Options opt = null;
    ColumnFamilyOptions cOpt = null;
    try {
      opt = new Options();
      cOpt = new ColumnFamilyOptions();
      opt.setMergeOperatorName("");
      cOpt.setMergeOperatorName("");
    } finally {
      if (opt != null) {
        opt.dispose();
      }
      if (cOpt != null) {
        cOpt.dispose();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullStringInSetMergeOperatorByNameOptions() {
    Options opt = null;
    try {
      opt = new Options();
      opt.setMergeOperatorName(null);
    } finally {
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void
      nullStringInSetMergeOperatorByNameColumnFamilyOptions() {
    ColumnFamilyOptions opt = null;
    try {
      opt = new ColumnFamilyOptions();
      opt.setMergeOperatorName(null);
    } finally {
      if (opt != null) {
        opt.dispose();
      }
    }
  }*/
}
