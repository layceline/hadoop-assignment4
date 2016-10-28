package hadoop.hbase;

/**
 * Created by celine on 27/10/2016.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAction {

    private Configuration conf = null;
    private static final String TABLE_NAME = "clay";
    private Connection connection;
    private Table table;

    public HBaseAction(){

        conf = HBaseConfiguration.create();
        //Adding HBase configuration file
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Function to connect HBase table
     * Using HTable is deprecated
     * @throws IOException
     */
    public void connectToTable() throws IOException{
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    /**
     * Function to disconnect from table and close the connection
     * @throws IOException
     */
    public void closeConnection() throws IOException{
        table.close();
        connection.close();
    }

    /**
     * Function to add a row to the table
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public void addRow(String rowKey, String family, String qualifier, String value) throws IOException{

        connectToTable();

        //Creating the Put object that I need to add a column to the rowKey
        //HBase is all about Bytes, so I need to convert every string into bytes
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

        //Commit the row to the table
        table.put(put);

        closeConnection();
    }

    /**
     * Function to delete a row from the table given the rowKey
     * @param rowKey
     * @throws IOException
     */
    public void deleteRow(String rowKey) throws IOException{

        connectToTable();

        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);

        table.delete(list);

        closeConnection();

    }

    /**
     * Function getting a specific row
     * @param rowKey
     * @throws IOException
     */
    public void getRow(String rowKey) throws IOException{

        connectToTable();

        //Creating the Get object needed to retrieve information
        Get get = new Get(Bytes.toBytes(rowKey));
        //Take all information from one row
        Result res = table.get(get);

        //Go through the results and print their values
        //Every data is in bytes
        for(Cell kv : res.listCells()){
            System.out.print(new String(kv.getFamilyArray()) + " ");
            System.out.print(new String(kv.getQualifierArray()) + " ");
            System.out.println(new String(kv.getValueArray()) + " ");
        }
        closeConnection();
    }

    public void getAllRows() throws IOException{

        connectToTable();

        //Creating a scan that does the same thing as the scan in hbase shell
        Scan s = new Scan();
        //Getting all rows
        ResultScanner ss = table.getScanner(s);
        //Loop to go over rows
        for(Result r:ss){
            //Loop to go over element of a row
            for(Cell kv : r.listCells()){
                System.out.print(new String(kv.getRowArray()) + " ");
                System.out.print(new String(kv.getFamilyArray()) + " ");
                System.out.print(new String(kv.getQualifierArray()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValueArray()));
            }
        }
        closeConnection();
    }

    /**
     * Function to get information of a specific column family of user
     * @param rowKey
     * @param family
     * @throws IOException
     */
    public void getFamily(String rowKey, String family) throws IOException{
        connectToTable();

        //Creating the Get object
        Get get = new Get(Bytes.toBytes(rowKey));
        //addFamily to specify which column family I want
        get = get.addFamily(Bytes.toBytes(family));
        //Getting the results
        Result res = table.get(get);

        //Going through the results
        for(Cell kv : res.listCells()){
            System.out.print(new String(kv.getQualifierArray()) + " ");
            System.out.println(new String(kv.getValueArray()));
        }
        closeConnection();

    }

    /**
     * Function to get information of a specific column family with qualifier
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public void getFamily(String rowKey, String family, String qualifier) throws IOException{
        connectToTable();

        //Creating the Get object
        Get get = new Get(Bytes.toBytes(rowKey));
        //addColumn to specify the column family and qualifier
        get = get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        //Getting the results
        Result res = table.get(get);
        //Going through the result
        for(Cell kv : res.listCells()){
            System.out.println(new String(kv.getValueArray()));
        }
        closeConnection();

    }

    /**
     * Function to check the existence of a specific row
     * @param rowKey
     * @return true if the row exists, false otherwise
     * @throws IOException
     */
    public boolean checkRow(String rowKey) throws IOException{

        connectToTable();

        //Creating the Get object needed to retrieve information
        Get get = new Get(Bytes.toBytes(rowKey));
        //Take all information from one row
        Result res = table.get(get);

        //Check if the result is null or not
        if(res.listCells() == null){
            closeConnection();
            return false;
        }
        else{
            closeConnection();
            return true;
        }
    }
}
