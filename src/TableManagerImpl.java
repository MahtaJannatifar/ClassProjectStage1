import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util. HashMap;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  @Override
  public StatusCode createTable(String tableName,  String[] attributeNames, AttributeType[] attributeType, String[] primaryKeyAttributeNames) {
    // your code
    if(tableName == null){
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    else if(attributeNames == null){
      return  StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    else if(attributeType == null){
      return  StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    else if(primaryKeyAttributeNames == null){
      return  StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }
    else if(attributeNames.length != attributeType.length){
      return StatusCode.TABLE_CREATION_DIFFERENT_SIZES;
    }
  //check if PK attribute name is  subset of attribute name if pk array is inside the atr name, and if not return not found
    int m=attributeNames.length,  n=primaryKeyAttributeNames.length;
    int k;
    int j;
    for ( k=0; k<n; k++){
      for ( j = 0; j<m; j++){
        if(primaryKeyAttributeNames[k].equals(attributeNames[j])) {
          break;
        }
      }
      if (j == m){
        return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }
    }

      FDB fdb = FDB.selectAPIVersion(710);
      Database db = null;

      try {
        db = fdb.open();
      } catch (Exception e) {
        System.out.println("ERROR: the database is not successfully opened: " + e);
      }
      Transaction tx = db.createTransaction();

      if( DirectoryLayer.getDefault().list(tx).join().contains(tableName)) {
        System.out.println(tableName+"  already exists!");
        return StatusCode.TABLE_ALREADY_EXISTS;
      }
      else{
        System.out.println(tableName+" does not exist, going to add to table.");
        final DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(tableName)).join();


        //need to add the table to fdb:
        Transaction insertionTx = db.createTransaction();


          boolean isPK = false;
          for (int i=0; i< attributeNames.length; i++) {
            String name = attributeNames[i];
            AttributeType type = attributeType[i];

            //if attribute name is inside primary key attribute name, it's a PK
            if (Arrays.asList(primaryKeyAttributeNames).contains(name)){
              System.out.println(name + " is a PK! ");
              isPK = true;
            }

            //tuple to convert atr name to byte array: create a tuple, 1 tuple for key and 1 tuple for value
            //it was name instead of attribute names
            Tuple keyTuple = Tuple.from(name);
            Tuple valueTuple = Tuple.from(isPK,type.toString());

            insertionTx.set(dir.pack(keyTuple),dir.pack(valueTuple));

          }
          //commit the changes to FDB
        insertionTx.commit().join();
          insertionTx.close();
          db.close();

        return StatusCode.SUCCESS;
      }
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
//
//
//
//    for(int i=0; i<listTables().size(); i++){
//
//    }
//
//    tx.close();
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    HashMap<String,TableMetadata> List_table = new HashMap<>();

    List<String> tableList = DirectoryLayer.getDefault().list(tx).join();
    System.out.println("Table list => "+ tableList);
    List<String> atrList = new ArrayList<>();
    List<Object> typesList = new ArrayList<>();
    List<String> primKeysList = new ArrayList<>();

    for(int i=0; i<tableList.size(); i++){
      String tableName = tableList.get(i);
      final DirectorySubspace dir = DirectoryLayer.getDefault().open(db, PathUtil.from(tableName)).join();
      Range range = dir.range();
      //list of all kv pairs
      List<KeyValue> kvs = tx.getRange(range).asList().join();

      for(int k=0; k<kvs.size(); k++)
      {
        Tuple keyTuple = dir.unpack(kvs.get(k).getKey());
        System.out.println("keyTuple: "+ keyTuple);
        Tuple valueTuple = dir.unpack( kvs.get(k).getValue());
        System.out.println("ValueTuple: "+  valueTuple);
        Object isPK = valueTuple.get(0);
        Object attrType = valueTuple.get(1);
        Object atrName = keyTuple.get(0);

        System.out.println( isPK + " isPK");
        System.out.println( attrType + " is type");

        if((boolean)isPK){

          primKeysList.add((String) keyTuple.get(0));
        }
        typesList.add( attrType);
        atrList.add((String) atrName);
      }
      System.out.println("PRIMARY KEYS LIST "+ primKeysList);
      String[] primArr = new String[primKeysList.size()];
      AttributeType[] typesArr = new AttributeType[atrList.size()];
      String[] atrArr = new String[atrList.size()];
      //put the prim keys in an array
      for(int j=0; i<primKeysList.size(); i++){
        primArr[j] = primKeysList.get(j);
      }
      //put the atrs and types into arrays
      for(int z=0; z<atrList.size(); z++){
        typesArr[z] = AttributeType.findByValue(String.valueOf(typesList.get(z)));
        atrArr[z] = atrList.get(z);
      }
      TableMetadata tmd = new TableMetadata(atrArr,  typesArr,  primArr);
      List_table.put(tableName,tmd);
      System.out.println(List_table);
    }
    tx.close();
    return  List_table;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }

    db.run(tx-> {
      final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
      final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
      tx.clear(st, en);
      return null;
    });
    return StatusCode.SUCCESS;
  }
}
