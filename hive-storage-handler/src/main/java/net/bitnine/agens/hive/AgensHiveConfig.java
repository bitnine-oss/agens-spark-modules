package net.bitnine.agens.hive;

// **참고 : hive-jdbc-storage-handler
public enum AgensHiveConfig {

    DATASOURCE("datasource", true),
    QUERY("query", true),

    LIVY_URL("livy", false),
    TEMP_PATH("temp", false),

    VERTEX_INDEX("vertex", false),
    EDGE_INDEX("edge", false),
    FETCH_SIZE("fetch.size", false),
    COLUMN_MAPPING("column.mapping", false);

    private String propertyName;
    private boolean required = false;

    AgensHiveConfig(String propertyName, boolean required) {
        this.propertyName = propertyName;
        this.required = required;
    }

    AgensHiveConfig(String propertyName) {
        this.propertyName = propertyName;
    }

    public String fullName() {
        return AgensHiveConfigManager.CONFIG_PREFIX + "." + propertyName;
    }

    public boolean isRequired() {
        return required;
    }

}
