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
    List<String> PKList = null;
    int k;
    int j;
    for ( k=0; k<n; k++){
      for ( j = 0; j<m; j++){
//        todo: SIZE of the atr names and the pklist is diffent=> null exception => fix!
        if(primaryKeyAttributeNames[k].equals(attributeNames[j]))
          // save all PK in the list
          PKList.add(primaryKeyAttributeNames[k]);
          break;
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
      final DirectorySubspace subdir = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(tableName)).join();
      Transaction tx = db.createTransaction();

      if( DirectoryLayer.getDefault().list(tx).join().contains(tableName)) {
        System.out.println(tableName+"  already exists!");
        return StatusCode.TABLE_ALREADY_EXISTS;
      }
      else{
        System.out.println(tableName+" does not exist, going to add to table.");

        //need to add the table to fdb:
        Transaction insertionTx = db.createTransaction();
        Boolean isPK = false;
        for (int i=0; i< attributeNames.length; i++) {
          String name = Arrays.toString(attributeNames);
          if (PKList.contains(Arrays.toString(attributeNames))){
            System.out.println("it is a PK! ");
            isPK = true;
          }
//          todo: check if this atr is PK( tuples) ? tuple to convert atr name to byte array: create a tuple, 1 tuple for key and 1 tuple for value
          insertionTx.set(Tuple.from(name).pack(),Tuple.from(isPK,atrType).pack());

          if (DirectoryLayer.getDefault().list(tx).join().size() > 0) {
              System.out.println("items are " + DirectoryLayer.getDefault().list(tx).join());
          }
        }
//        commit the changes to FDB
        insertionTx.commit();
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
    for (String tableName : tableList) {
      //      get all the KV pair under tableName directory, get directory of FDB with this name (create())
      final DirectorySubspace subdir = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(tableName)).join();
//      get all the kv pairs under the subdir, k: name, c: bool. itr over list of kv pair get key and value if value = t should be part of PK.
      //todo: have a list of all the PK and add to tableMetaData(atr,PKlist)
      // key is attribute names, collect all keys under a list
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

    return StatusCode.SUCCESS;
  }
}
