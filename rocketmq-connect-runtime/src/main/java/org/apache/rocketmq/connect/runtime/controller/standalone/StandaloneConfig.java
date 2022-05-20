package org.apache.rocketmq.connect.runtime.controller.standalone;

import org.apache.rocketmq.connect.runtime.config.ConnectConfig;

/**
 *  worker standalone config
 */
public class StandaloneConfig extends ConnectConfig  {
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";
    private static final String OFFSET_STORAGE_FILE_FILENAME_DOC = "File to store offset data in";

    private String offsetStorageFileFilename;

    public String getOffsetStorageFileFilename() {
        return offsetStorageFileFilename;
    }

    public void setOffsetStorageFileFilename(String offsetStorageFileFilename) {
        this.offsetStorageFileFilename = offsetStorageFileFilename;
    }
}
