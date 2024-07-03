package com.alibaba.datax.plugin.reader.gcsreader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gcsreader.GcsReader;
import com.alibaba.datax.plugin.reader.gcsreader.GcsReaderErrorCode;
import com.alibaba.datax.plugin.reader.gcsreader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT;

public class DFSUtil {
    private static final Logger LOG = LoggerFactory.getLogger(GcsReader.Job.class);

    private enum Type {
        STRING, LONG, BOOLEAN, DOUBLE, DATE, TIMESTAMP_MICROS, DATE_INT32
    }

    public static void readParquet(String sourceParquetFilePath, Configuration readerSliceConfig, RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        LOG.info(String.format("Start Read parquetfile [%s].", sourceParquetFilePath));
        String nullFormat = readerSliceConfig.getString(NULL_FORMAT);
        final String jsonKeyPath = readerSliceConfig.getString(Key.JSON_KEY_PATH);
        final String bucket = readerSliceConfig.getString(Key.BUCKET);

        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        boolean isReadAllColumns = false;
        int columnIndexMax = -1;
        // 判断是否读取所有列
        if (null == column || column.size() == 0) {
            throw new IllegalArgumentException("参数错误, 必须配置 column");
        } else {
            columnIndexMax = getMaxIndex(column);
        }
        for (int i = 0; i <= columnIndexMax; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != columnIndexMax) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        if (columnIndexMax >= 0) {

            LOG.info("builder hadoop configuration");
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
            conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
            conf.set("google.cloud.auth.service.account.enable", "true");
            conf.set("google.cloud.auth.service.account.json.keyfile", jsonKeyPath);

            try {
                LOG.info("builder hadoop google  filesystem");
                FileSystem fs = FileSystem.get(URI.create("gs://" + bucket), conf);

                LOG.info("builder hadoop google  filesystem success");

                Path parquetFilePath = new Path("gs://" + bucket + "/" + sourceParquetFilePath);

                GroupReadSupport support = new GroupReadSupport();
                ParquetReader<Group> reader = ParquetReader.builder(support, parquetFilePath).withConf(conf).build();
                Group line = null;
                List<Object> recordFields;
                while ((line = reader.read()) != null) {
                    recordFields = new ArrayList<>();
                    // 从line中获取每个字段
                    for (int i = 0; i <= columnIndexMax; i++) {
                        Object field;

                        try {
                            field = line.getValueToString(i, 0);
                        } catch (RuntimeException e) {
                            field = null;
                        }

                        recordFields.add(field);
                    }
                    transportOneRecord(column, recordFields, recordSender,
                            taskPluginCollector, isReadAllColumns, nullFormat);
                }
                reader.close();
            } catch (Exception e) {

                String message = String.format("从parquetfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceParquetFilePath);
                LOG.error(message);
                // throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        }
    }

    private static int getMaxIndex(List<ColumnEntry> columnConfigs) {
        int maxIndex = -1;
        for (ColumnEntry columnConfig : columnConfigs) {
            Integer columnIndex = columnConfig.getIndex();
            if (columnIndex != null && columnIndex < 0) {
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index,column配置:%s",
                        JSON.toJSONString(columnConfigs));
                LOG.error(message);
                throw DataXException.asDataXException(GcsReaderErrorCode.BAD_CONFIG_VALUE, message);
            } else if (columnIndex != null && columnIndex > maxIndex) {
                maxIndex = columnIndex;
            }
        }
        return maxIndex;
    }

    private static void transportOneRecord(List<ColumnEntry> columnConfigs, List<Object> recordFields
            , RecordSender recordSender, TaskPluginCollector taskPluginCollector, boolean isReadAllColumns, String nullFormat) {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        try {
            if (isReadAllColumns) {
                // 读取所有列，创建都为String类型的column
                for (Object recordField : recordFields) {
                    String columnValue = null;
                    if (recordField != null) {
                        columnValue = recordField.toString();
                    }
                    columnGenerated = new StringColumn(columnValue);
                    record.addColumn(columnGenerated);
                }
            } else {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue = null;

                    if (null != columnIndex) {
                        if (null != recordFields.get(columnIndex))
                            columnValue = recordFields.get(columnIndex).toString();
                    } else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (StringUtils.equals(columnValue, nullFormat)) {
                        columnValue = null;
                    }
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = new DateColumn((Date) null);
                                } else {
                                    String formatString = columnConfig.getFormat();
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换
                                        SimpleDateFormat format = new SimpleDateFormat(
                                                formatString);
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    } else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;

                        case TIMESTAMP_MICROS:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = null;
                                } else {
                                    // 框架尝试转换
                                    columnGenerated = new DateColumn(
                                            Long.parseLong(columnValue) / 1000);
                                }
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将 TIMESTAMP_MICROS [%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        case DATE_INT32:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = null;
                                } else {
                                    // 框架尝试转换
                                    LocalDate baseDate = LocalDate.of(1970, 1, 1);
                                    // 加上的天数
                                    // 计算新日期
                                    columnGenerated = new DateColumn(java.sql.Date.valueOf(baseDate.plusDays(Integer.parseInt(columnValue))));
                                }
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将 TIMESTAMP_MICROS [%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }

                    record.addColumn(columnGenerated);
                }
            }
            recordSender.sendToWriter(record);
        } catch (IllegalArgumentException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        } catch (IndexOutOfBoundsException ioe) {
            taskPluginCollector
                    .collectDirtyRecord(record, ioe.getMessage());
        } catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }

        // return record;
    }

}
