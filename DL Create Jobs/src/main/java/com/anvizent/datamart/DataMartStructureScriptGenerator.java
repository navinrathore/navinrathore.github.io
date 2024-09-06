package com.anvizent.datamart;

public class DataMartStructureScriptGenerator {
    private DataSourceType dataSourceType;
    private String dlId;

    public DataMartStructureScriptGenerator(DataSourceType type, String dlId) {
        this.dataSourceType = type;
        this.dlId = dlId;
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public String getDlId() {
        return dlId;
    }

    public class DataMartConfigScriptGenerator {
        public DataMartConfigScriptGenerator() {
         }

        public void generateConfigScript() {
         }
    }

    public class DataMartValueScriptGenerator {
        public DataMartValueScriptGenerator() {
         }

        public void generateValueScript() {
         }
    }

    public class DataMartAlterScriptGenerator {
        public DataMartAlterScriptGenerator() {
         }

        public void generateAlterScript() {
         }
    }

    public class DataMartSavedScriptGenerator {
        public DataMartSavedScriptGenerator() {
         }

        public void generateSavedScript() {
         }
    }

    public class DataMartCreateScriptGenerator {
        public DataMartCreateScriptGenerator() {
        }

        public void generateCreateScript() {
            getDataSourceType();
            dataSourceType = DataSourceType.MYSQL;
            getDlId();
         }
    }
}
