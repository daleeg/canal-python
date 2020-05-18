# -*- coding: utf-8 -*-
from __future__ import unicode_literals
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
