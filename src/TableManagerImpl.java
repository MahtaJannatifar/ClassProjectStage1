import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;

import java.util.HashMap;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    // your code

    System.out.println("blabla---");
    if(tableName == null){
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    else if(attributeNames == null){
      return  StatusCode.TABLE_CREATION_ATTRIBUTE_NAME_INVALID;
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
//    else, table can be created:
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
//    now can create table:
      DirectorySubspace subdir = null;
      subdir = DirectoryLayer.getDefault().create(db,PathUtil.from(tableName)).join();
      ReadTransactionContext tc;
      if(DirectoryLayer.getDefault().list() == subdir){
        System.out.println("table exists");
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
