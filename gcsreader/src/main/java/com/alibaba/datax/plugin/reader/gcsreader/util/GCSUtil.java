package com.alibaba.datax.plugin.reader.gcsreader.util;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.auth.oauth2.GoogleCredentials;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gcsreader.Key;
import com.alibaba.datax.plugin.reader.gcsreader.GcsReaderErrorCode;


import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage;


public class GCSUtil {
    public static Storage initGCSClient(Configuration conf)  {
        String jsonKeyPath = conf.getString(Key.JSON_KEY_PATH);
        String bucket = conf.getString(Key.BUCKET);
        String pathPrefix = conf.getString(Key.PATH_PREFIX);


        // String jsonPath = "/path/to/your-service-account-file.json";

        // 使用服务账户密钥文件创建认证凭据
        GoogleCredentials credentials = null;
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyPath));
        } catch (IOException e) {

            throw DataXException.asDataXException(
                    GcsReaderErrorCode.GCS_FILE_NO_FOUND, e.getMessage());
        }

        // 使用认证凭据初始化 Google Cloud Storage 客户端
        Storage client = null;
        try {
            client = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    GcsReaderErrorCode.ILLEGAL_VALUE, e.getMessage());
        }
        return client;
    }
}
