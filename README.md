# pycanal

## 一.pycanal 简介

pycanal 是阿里巴巴开源项目 [Canal](https://github.com/alibaba/canal)是阿里巴巴mysql数据库binlog的增量订阅&消费组件 的 python 客户端。为 python 开发者提供一个更友好的使用 Canal 的方式。Canal 是mysql数据库binlog的增量订阅&消费组件。

基于日志增量订阅&消费支持的业务：

1. 数据库镜像
2. 数据库实时备份
3. 多级索引 (卖家和买家各自分库索引)
4. search build
5. 业务cache刷新
6. 价格变化等重要业务消息

关于 Canal 的更多信息请访问 https://github.com/alibaba/canal/wiki

## 二.应用场景

pycanal 作为Canal的客户端，其应用场景就是Canal的应用场景。关于应用场景在Canal介绍一节已有概述。举一些实际的使用例子：

1.代替使用轮询数据库方式来监控数据库变更，有效改善轮询耗费数据库资源。

2.根据数据库的变更实时更新搜索引擎，比如电商场景下商品信息发生变更，实时同步到商品搜索引擎 Elasticsearch、solr等

3.根据数据库的变更实时更新缓存，比如电商场景下商品价格、库存发生变更实时同步到redis

4.数据库异地备份、数据同步

5.根据数据库变更触发某种业务，比如电商场景下，创建订单超过xx时间未支付被自动取消，我们获取到这条订单数据的状态变更即可向用户推送消息。

6.将数据库变更整理成自己的数据格式发送到kafka等消息队列，供消息队列的消费者进行消费。

## 三.工作原理

pycanal  是 Canal 的 python 客户端，它与 Canal 是采用的Socket来进行通信的，传输协议是TCP，交互协议采用的是 Google Protocol Buffer 3.0。

## 四.工作流程

1.Canal连接到mysql数据库，模拟slave

2.pycanal 与 Canal 建立连接

2.数据库发生变更写入到binlog

5.Canal向数据库发送dump请求，获取binlog并解析

4.pycanal 向 Canal 请求数据库变更

4.Canal 发送解析后的数据给pycanal

5.pycanal收到数据，消费成功，发送回执。（可选）

6.Canal记录消费位置。

## 五.快速启动

### 安装Canal

Canal 的安装以及配置使用请查看 https://github.com/alibaba/canal/wiki/QuickStart

### 环境要求
python >= 3

### 构建canal python客户端

````shell
pip install py3canal
````

### 建立与Canal的连接
````python
import time
from pycanal.client import CanalClient
from pycanal.protocol import EntryProtocol_pb2
from tornado.ioloop import IOLoop


async def main():
    while True:
        canal = CanalClient(host="10.88.190.151", port=21111)
        await canal.connect()
        await canal.check_valid()
        await canal.subscribe(client_id=b"14001", destination=b"pandora")
        while True:
            try:
                message = await canal.get(100)
                entries = message["entries"]
                for entry in entries:
                    entry_type = entry.entryType
                    if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                                      EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                        continue
                    row_change = EntryProtocol_pb2.RowChange()
                    row_change.MergeFromString(entry.storeValue)
                    header = entry.header
                    database = header.schemaName
                    table = header.tableName
                    event_type = header.eventType

                    for row in row_change.rowDatas:
                        if event_type == EntryProtocol_pb2.EventType.DELETE:
                            format_data = dict()
                            print("delete: {}".format(EntryProtocol_pb2.EventType.DELETE))
                            for column in row.beforeColumns:
                                format_data[column.name] = column.value
                            data = dict(
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            )
                            print(data)
                        elif event_type == EntryProtocol_pb2.EventType.INSERT:
                            format_data = dict()
                            print("insert: {}".format(EntryProtocol_pb2.EventType.INSERT))
                            for column in row.afterColumns:
                                format_data[column.name] = column.value
                            data = dict(
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            )
                            print(data)
                        else:
                            format_data = dict()
                            format_data["before"] = format_data["after"] = dict()
                            for column in row.beforeColumns:
                                format_data["before"][column.name] = column.value
                            for column in row.afterColumns:
                                format_data["after"][column.name] = column.value
                            data = dict(
                                db=database,
                                table=table,
                                event_type=event_type,
                                data=format_data,
                            )
                            print(data)
            except Exception as e:
                print(e)
                break
            time.sleep(1)

        canal.disconnect()
        time.sleep(2)


if __name__ == "__main__":
    io_loop = IOLoop.current()
    io_loop.run_sync(main)
````

更多详情请查看 [Sample](https://github.com/vallee11/canal-python/canal/example.py)

