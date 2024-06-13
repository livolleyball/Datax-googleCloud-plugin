package com.alibaba.datax.plugin.reader.gcsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gcsreader.util.GCSUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GcsReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(GcsReader.Job.class);
        private Configuration readerOriginConfig = null;
        private Storage storgeClient = null;

        private String bucket;
        private String pathPrefix;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        private void validate() {

            storgeClient = GCSUtil.initGCSClient(this.readerOriginConfig);

            bucket = this.readerOriginConfig.getString(Key.BUCKET);
            pathPrefix = this.readerOriginConfig.getString(Key.PATH_PREFIX);
            if (StringUtils.isBlank(bucket)) {
                throw DataXException.asDataXException(
                        GcsReaderErrorCode.CONFIG_INVALID_BUCKET,
                        "invalid bucket");
            } else {
                Bucket bucket = storgeClient.get(this.bucket);

                if (bucket == null) {
                    throw DataXException.asDataXException(
                            GcsReaderErrorCode.CONFIG_INVALID_BUCKET,
                            "invalid bucket");
                }

            }
        }

        @Override
        public void prepare() {
            LOG.debug("prepare()");

        }

        @Override
        public void post() {

            LOG.debug("post()");
        }

        @Override
        public void destroy() {

            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");

            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // 将每个单独的 object 作为一个 slice
            List<String> paths = getDirBlobList();
            if (0 == paths.size()) {
                throw DataXException.asDataXException(
                        GcsReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format(
                                "未能找到待读取的Object,请确认您的配置项bucket: %s dir: %s",
                                this.readerOriginConfig.get(Key.BUCKET),
                                this.readerOriginConfig.get(Key.PATH_PREFIX)));
            }

            for (String path : paths) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Constant.OBJECT_PATH, path);
                readerSplitConfigs.add(splitedConfig);
                LOG.info(String.format("gcs object to be read:%s", path));
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private List<String> getDirBlobList() {

            List<String> remoteObjectListings = new ArrayList<String>();
            Bucket bucket = storgeClient.get(this.bucket);
            Iterable<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(pathPrefix)).iterateAll();



            for (Blob blob : blobs) {
                if (!blob.isDirectory() && isDirectory(blob.getName())) {
                    remoteObjectListings.add(blob.getName());
                }
            }
            return remoteObjectListings;
        }
    }

    private static boolean isDirectory(String path) {
        // 正则表达式，匹配连续数字
        Pattern pattern = Pattern.compile("-(\\d+)\\.csv");
        Matcher matcher = pattern.matcher(path);

        if (matcher.find()) {
            String numberStr = matcher.group(1);  // 匹配的全部数字字符串 (包含前导零)
            int number = Integer.parseInt(numberStr);  // 转换为整数自动去除前导零

            return number >= 300 && number <350;
        } else {
            System.out.println("No number found in the file name.");
            return false;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

        private Configuration readerSliceConfig;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

            LOG.debug("read start");
            String path = readerSliceConfig.getString(Constant.OBJECT_PATH);
            Storage gcsClient = GCSUtil.initGCSClient(readerSliceConfig);

            final Bucket bucket = gcsClient.get(readerSliceConfig.getString(Key.BUCKET));
            Blob blob = bucket.get(path);

            if (blob != null) {
                InputStream inputStream = readBlobAsInputStream(blob);
                UnstructuredStorageReaderUtil.readFromStream(inputStream, path,
                        this.readerSliceConfig, recordSender,
                        this.getTaskPluginCollector());
                recordSender.flush();
            } else {
                System.out.println("Blob not found.");
            }
        }

        private static InputStream readBlobAsInputStream(Blob blob) {
            return Channels.newInputStream(blob.reader());
        }
    }
}