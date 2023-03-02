import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

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
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    // your code
    System.out.println("----");
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
    else{
      FDB fdb = FDB.selectAPIVersion(710);
      Database db = null;
      DirectorySubspace rootDirectory = null;

      try {
        db = fdb.open();
      } catch (Exception e) {
        System.out.println("ERROR: the database is not successfully opened: " + e);
      }

      System.out.println("Open FDB Successfully!");
      // initialize root directory, which stands for the Company
      try {
        rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
                PathUtil.from("root")).join();
      } catch (Exception e) {
        System.out.println("ERROR: the root directory is not successfully opened: " + e);
      }
      System.out.println("Created root directory successfully!");
      // if the subdirectory does not exist, add it
      // initialize two subdirectories under the company, Employee and Department
      final DirectorySubspace subdir = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
      Transaction tx = db.createTransaction();
      if(subdir == DirectoryLayer.getDefault().list(tx).join()) {
        System.out.println("name: "+ tableName);
        System.out.println("table already exists");
        return StatusCode.TABLE_ALREADY_EXISTS;
      }
      else{
        System.out.println("does not exist");
        //need to add the table to fdb:
//        for (e in tableName) {
          Transaction insertionTx = db.createTransaction();
          addAttributeValuePairToTable(insertionTx, subdir, primaryKeyAttributeNames[0],attributeNames[0], 123);
        System.out.println("inserted subdir is "+subdir);
        if( DirectoryLayer.getDefault().list(tx).join().size() >0) {
          System.out.println("size is "+  DirectoryLayer.getDefault().list(tx).join().size());
        }
//          addAttributeValuePairToTable(insertionTx, employeeTable,
//                  ssn, Employee.EMPLOYEE_ATTRIBUTE_NAME, e.att());
//          addAttributeValuePairToTable(insertionTx, employeeTable,
//                  ssn, Employee.EMPLOYEE_ATTRIBUTE_SALARY, e.getSalary());
//          addAttributeValuePairToTable(insertionTx, employeeTable,
//                  ssn, Employee.EMPLOYEE_ATTRIBUTE_DNO, e.getDno());
//          insertionTx.commit().join();
//        }

      }

    }

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    return null;
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
