import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class LocalStackMain {
    static AmazonDynamoDB client= AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566","us-west-2")).build();
    static DynamoDB dynamodb=new DynamoDB(client);
    static String tableName="testtable";
    public static void createTable(){
        try {
            System.out.println("Table Creating .....");
            Table table = dynamodb.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("year", KeyType.HASH),
                            new KeySchemaElement("title", KeyType.RANGE)),
                    Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N), new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));

            System.out.println("Table " + tableName + " Created!!!" + "with status " + (table.getDescription() != null ? table.getDescription().getTableStatus() : "Inactive"));
            table.waitForActive();
            System.out.println("Table " + tableName +"Created!!!" +"with Status " + table.getDescription().getTableStatus());
        }
        catch(Exception e)
        {
            System.out.println("Unable to create Table!!!!");
        }
    }
    public static void insertData() {
        int year = 2016;
        String title = "The Big New Movie 2";

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);
        try{
            Table table=dynamodb.getTable(tableName);
            PutItemOutcome outcome = table
                    .putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        }catch(Exception E){
            System.out.println("Error Inserting Code");
        }

    }
    public static  void deleteTable(){
        try {
            dynamodb.getTable(tableName).delete();
            System.out.println("Your table is no longer in our records.....");
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
    }
    public static void getTables(){

        TableCollection<ListTablesResult> s=   dynamodb.listTables();
        Iterator<Table> iterator=s.iterator();

        System.out.println("Printing Tables");
        while(iterator.hasNext()){
            Table t=iterator.next();
            System.out.println(t.getTableName());
        }


    }
    public static void scanTable(){
        System.out.println("Scanning the Table.....");
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName);

        ScanResult result = client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()){
            System.out.println(item);
        }
    }
    public static void main(String[] args) {
        //System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,"true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,"true");
        createTable();
        insertData();
        scanTable();
        getTables();
        deleteTable();


    }
    }
