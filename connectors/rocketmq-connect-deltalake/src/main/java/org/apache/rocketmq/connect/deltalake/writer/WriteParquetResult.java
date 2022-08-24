package org.apache.rocketmq.connect.deltalake.writer;

/**
 * @author osgoo
 * @date 2022/8/24
 */
public class WriteParquetResult {
    private String tableDir;
    private String fullFileName;
    private boolean needAddFile;
    public WriteParquetResult(String tableDir, String fullFileName, boolean needAddFile) {
        this.tableDir = tableDir;
        this.fullFileName = fullFileName;
        this.needAddFile = needAddFile;
    }

    public String getTableDir() {
        return tableDir;
    }

    public void setTableDir(String tableDir) {
        this.tableDir = tableDir;
    }

    public String getFullFileName() {
        return fullFileName;
    }

    public void setFullFileName(String fullFileName) {
        this.fullFileName = fullFileName;
    }

    public boolean isNeedAddFile() {
        return needAddFile;
    }

    public void setNeedAddFile(boolean needAddFile) {
        this.needAddFile = needAddFile;
    }
}
