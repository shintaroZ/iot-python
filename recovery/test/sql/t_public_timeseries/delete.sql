delete from %(tableName)s
where RECEIVED_DATETIME between '%(receivedDatetimeBefore)s' and '%(receivedDatetimeAfter)s';
