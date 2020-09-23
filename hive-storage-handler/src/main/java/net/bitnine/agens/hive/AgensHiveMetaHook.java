package net.bitnine.agens.hive;

import com.google.common.base.Strings;

import net.bitnine.agens.livy.RunCypherJob;
import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.util.AgensLivyJobException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.*;

public class AgensHiveMetaHook implements HiveMetaHook {

    public static final Log LOG = LogFactory.getLog(AgensHiveMetaHook.class);

    private static final String DEFAUT_TEMP_PATH = "/user/agens/temp";

    /**
     * Performs required validations prior to creating the table
     *
     * @param table Represents hive table object
     * @throws MetaException if table metadata violates the constraints
     */
    @Override
    public void preCreateTable(Table table) throws MetaException {
        // convert table properties to properties object
        Properties properties = new Properties();
        properties.putAll(table.getParameters());

        // Check all mandatory table properties and copy to jobProperties
        Map<String, String> jobProperties = new HashMap<>();
        AgensHiveConfigManager.copyConfigurationToJob(properties, jobProperties);

        // for DEBUG
        System.err.printf("CreateTable: %s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName());
        System.err.println("parameters ==> "+jobProperties);

        StorageDescriptor sd = table.getSd();
        // remove columns
        sd.setCols(new ArrayList<FieldSchema>());
        // remove avro default schema (actually this is a dummy)
        table.getParameters().remove("avro.schema.url");

        // ** required jobs for creating external table using avro
        // 1) cols          => empty list
        // 2) inputFormat   => org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat,
        //    outputFormat  => org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat,
        // 3) location      => hdfs://minmac:9000/user/agens/temp/person.avro,
        // 4) schema        => avro.schema.literal

        // set formats for avro input and output
        sd.setInputFormat(AvroContainerInputFormat.class.getCanonicalName());
        sd.setOutputFormat(AvroContainerOutputFormat.class.getCanonicalName());

        // set properties for CypherJob
        String name = table.getTableName();
        String tempPath = jobProperties.getOrDefault(
                AgensHiveConfig.TEMP_PATH.fullName(), AgensHiveConfigManager.defaultTempPathFromHiveConf());
        String livyUrl = jobProperties.getOrDefault(
                AgensHiveConfig.LIVY_URL.fullName(), AgensHiveConfigManager.defaultLivyUrlFromHiveConf());
        String datasource = jobProperties.get(AgensHiveConfig.DATASOURCE.fullName());
        String query = jobProperties.get(AgensHiveConfig.QUERY.fullName());

        // set location of avro result after executing cypher
        sd.setLocation(AgensLivyHelper.savePath(tempPath, name));
        System.err.println("savePath ==> "+AgensLivyHelper.savePath(tempPath, name));
        System.err.println("");

        try {
            String schemaJson = RunCypherJob.run(livyUrl, datasource, name, query);
            table.getParameters().put("avro.schema.literal", schemaJson);
        }
        catch (AgensLivyJobException ex){
            System.err.println("Error: AgensLivyJob exception => "+ex.getMessage());
            throw new MetaException("[AgensLivyJob] Fail preCreateTable, because => "+ex.getMessage());
        }
        catch (Exception ex){
            System.err.println("Error: abnormal exception => "+ex.getMessage());
            throw new MetaException("[AgensLivyJob] Fail preCreateTable, because unknown reasons => "+ex.getMessage());
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        // instead of UnsupportedOperationException
        throw new MetaException("AgensHiveStorageHandler cannot support Rollbak Create Table : " +
                String.format("%s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName()));
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        // Do nothing by default
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        // Do nothing by default
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        // instead of UnsupportedOperationException
        throw new MetaException("AgensHiveStorageHandler cannot support Rollbak Drop Table : " +
                String.format("%s.%s.%s\n", table.getDbName(), table.getOwner(), table.getTableName()));
    }

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {
        // Do nothing by default
    }
}
