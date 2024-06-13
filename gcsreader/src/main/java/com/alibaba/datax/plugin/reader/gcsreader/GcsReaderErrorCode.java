package com.alibaba.datax.plugin.reader.gcsreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by mengxin.liumx on 2014/12/7.
 */
public enum GcsReaderErrorCode implements ErrorCode {
    // TODO: 修改错误码类型
    RUNTIME_EXCEPTION("GcsReader-00", "运行时异常"),
    GCS_FILE_NO_FOUND("GcsFileReader-01", "GCS jsonKeyFile Not Found"),
    CONFIG_INVALID_BUCKET("GcsFileReader-02", "bucket 为配置空 或者 bucket 不存在"),

    ILLEGAL_VALUE("GcsReader-06", "值错误"),

    EMPTY_BUCKET_EXCEPTION("OssReader-10", "您尝试读取的Bucket为空");

    private final String code;
    private final String description;

    private GcsReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}