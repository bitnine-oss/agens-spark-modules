package net.bitnine.agens.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

// **참고 : hive-jdbc-storage-handler
public class AgensHiveConfigManager {

    public static final String CONFIG_PREFIX = "agens.spark";

    public static final String DEFAULT_LIVY = "http://minmac:8998";
    public static final String DEFAULT_TEMP = "/user/agens/temp";

    private static final EnumSet<AgensHiveConfig> REQUIRED_PROPERTIES = EnumSet.of(
                AgensHiveConfig.LIVY_URL,
                AgensHiveConfig.DATASOURCE,
                AgensHiveConfig.TEMP_PATH,
                AgensHiveConfig.QUERY
    );

    private AgensHiveConfigManager() {
    }

    // extract Agens props from All props and copy to jobProps
    public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps) {
        checkRequiredPropertiesAreDefined(props);
        for (AgensHiveConfig configKey : REQUIRED_PROPERTIES) {
            String propertyKey = configKey.fullName();
            if( props.containsKey(propertyKey) ) jobProps.put(propertyKey, props.getProperty(propertyKey));
        }
    }

    // copy properties to Hadoop configuration
    public static Configuration convertPropertiesToConfiguration(Properties props) {
        checkRequiredPropertiesAreDefined(props);
        Configuration conf = new Configuration();
        for (Entry<Object, Object> entry : props.entrySet()) {
            conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return conf;
    }

    // check properties. if not satisfied, then raise Exception
    private static void checkRequiredPropertiesAreDefined(Properties props) {
        List<AgensHiveConfig> requiredProps = REQUIRED_PROPERTIES.stream()
                                    .filter(p->p.isRequired()).collect(Collectors.toList());
        for (AgensHiveConfig configKey : requiredProps) {
            String propertyKey = configKey.fullName();
            if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
                throw new IllegalArgumentException("Property '" + propertyKey + "' is required.");
            }
        }
    }

    public static String getConfigValue(AgensHiveConfig key, Configuration config) {
        return config.get(key.fullName());
    }

    private static boolean isEmptyString(String value) {
        return ((value == null) || (value.trim().isEmpty()));
    }

    public static String defaultLivyUrlFromHiveConf(){
        HiveConf conf = SessionState.getSessionConf();    // .get().getConf();
        return conf.get(AgensHiveConfig.LIVY_URL.fullName(), DEFAULT_LIVY);
    }

    public static String defaultTempPathFromHiveConf(){
        HiveConf conf = SessionState.getSessionConf();    // .get().getConf();
        return conf.get(AgensHiveConfig.TEMP_PATH.fullName(), DEFAULT_TEMP);
    }
}
