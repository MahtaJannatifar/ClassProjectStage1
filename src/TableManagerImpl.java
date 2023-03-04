import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;
import java.util. HashMap;

import java.util.HashMap;
import java.util.List;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType, String[] primaryKeyAttributeNames) {
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
            String type = attributeType[i].toString();

            //if attribute name is inside primary key attribute name, it's a PK
            if (Arrays.asList(primaryKeyAttributeNames).contains(name)){
              System.out.println(name + " is a PK! ");
              isPK = true;
            }

            //tuple to convert atr name to byte array: create a tuple, 1 tuple for key and 1 tuple for value
            insertionTx.set(Tuple.from(name).pack(),Tuple.from(isPK,type).pack());
          }
          //commit the changes to FDB
        insertionTx.commit().join();
        System.out.println("FDB items are " + DirectoryLayer.getDefault().list(insertionTx).join());


        return StatusCode.SUCCESS;
      }
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
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
    HashMap<String,TableMetadata> List_table = new HashMap <String,TableMetadata>();
    TableMetadata tmd = new TableMetadata();
//    todo: get the actual metadata for tmd -> use fdb based on how the data is stored
    List<String> tableList = DirectoryLayer.getDefault().list(tx).join();

    for(int i=0; i<tableList.size(); i++){
      String tableName = tableList.get(i);
      //      get all the KV pair under tableName directory, get directory of FDB with this name (create())
      final DirectorySubspace subdir = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(tableName)).join();
//      get all the kv pairs under the subdir, k: name, v: (bool,type). itr over list of kv pair get key and value if value = t should be part of PK.
      //todo: have a list of all the PK and add to tableMetaData(atrNames, atrValues, PKs)
      // key is attribute names, collect all keys under a list
      System.out.println("SUB DIR Get KEY: "+Arrays.toString(subdir.get(i).getKey()));
      //List_table.put(tableName,new TableMetadata(attributeNames,  attributeTypes,  primaryKeys));
    }
//    todo: for each table name get key value pairs: get all key value pair under a certain directory (list of KV pairs), key: atr name
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
    System.out.println("DROP tables is being called");
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
    System.out.println("DROPPED");
    return StatusCode.SUCCESS;
  }
}
