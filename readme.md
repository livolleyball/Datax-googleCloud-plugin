datax plugin for google cloud 
============================

- [X]  读取bucket 指定目录下的 csv 文件
- [ ]  使用bigquery query reader 
    - [ ] 使用bigqeury 进行查询
    - [ ] 结果数据写入临时表
    - [ ] 将临时表写入google cloud storage ,复用 gcs reader 的逻辑
    - [ ] 删除临时表 及 google cloud storage 中 特定目录下的临时文件

- [ ]  使用bigquery table reader
    - [ ] 将bigquery table  或者 bigqeury table 的特定partition 写入google cloud storage ,复用 gcs reader 的逻辑
    - [ ] 删除临时表 及 google cloud storage 中 特定目录下的临时文件




## 开发测试
正式打包时 ，需将 module 下 pom 文件中的 <build></build> 中的 resources 注釋掉
```xml
        <resources>
            <!--将resource目录也输出到target-->
            <resource>
                <directory>src/main/resources</directory>
                <includes>
                    <include>**/*.*</include>
                </includes>
                <filtering>true</filtering>
            </resource>
        </resources>
```
