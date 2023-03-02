import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import java.util. HashMap;

import java.util.HashMap;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  public static void addAttributeValuePairToTable(Transaction tx, DirectorySubspace table, String primaryKey,
                                                  String attributeName, Object attributeValue) {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(primaryKey).add(attributeName);

    Tuple valueTuple = new Tuple();
    valueTuple = valueTuple.addObject(attributeValue);
    tx.set(table.pack(keyTuple), valueTuple.pack());
  }

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
//    todo: figure out condition for this status code:
    else if(primaryKeyAttributeNames.length == 0 ){
      System.out.println("NO PRIMARY KEY_______");
      return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    }

      FDB fdb = FDB.selectAPIVersion(710);
      Database db = null;
      DirectorySubspace rootDirectory = null;

      try {
        db = fdb.open();
      } catch (Exception e) {
        System.out.println("ERROR: the database is not successfully opened: " + e);
      }
      // initialize root directory, which stands for the Company
      try {
        rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
                PathUtil.from("root")).join();
      } catch (Exception e) {
        System.out.println("ERROR: the root directory is not successfully opened: " + e);
      }
      // if the subdirectory does not exist, add it
      // initialize two subdirectories under the company, Employee and Department
      final DirectorySubspace subdir = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
      Transaction tx = db.createTransaction();

      if( DirectoryLayer.getDefault().list(tx).join().contains(tableName)) {
        System.out.println(tableName+"  already exists,cannot create a table with existing name!");
        return StatusCode.SUCCESS;
      }
      else{
        System.out.println(tableName+" does not exist, going to add to table.");

        //need to add the table to fdb:
        for (int i=0; i< DirectoryLayer.getDefault().list(tx).join().size(); i++) {
          Transaction insertionTx = db.createTransaction();
          addAttributeValuePairToTable(insertionTx, subdir, primaryKeyAttributeNames[i], attributeNames[i],"value" );
          if (DirectoryLayer.getDefault().list(tx).join().size() > 0) {
              System.out.println("items are " + DirectoryLayer.getDefault().list(tx).join());
          }
        }
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
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    HashMap<String,TableMetadata> List_table = new HashMap <String,TableMetadata>();
    TableMetadata tmd = new TableMetadata();
    List_table.put(DirectoryLayer.getDefault().list(tx).join().toString(), tmd);
    System.out.println("--- List size: "+ List_table.size());
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
    return StatusCode.SUCCESS;
  }
}
