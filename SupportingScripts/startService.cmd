cd
start HS2\hive-1.2.1.2.3.3.1-5\bin\hive --service metastore -p 2000
ping 192.0.2.2 -n 1 -w 40000 > nul
start HS2\hive-1.2.1.2.3.3.1-5\bin\hive --service hiveserver2
ping 192.0.2.2 -n 1 -w 100000 > nul
HS2\hive-1.2.1.2.3.3.1-5\bin\beeline -u jdbc:hive2://localhost:10001/;transportMode=http;hive.server2.servermode=http -n tester -p password -f script.q 
REM > beelineoutput.txt
FOR /F %a IN ('POWERSHELL -COMMAND "$([guid]::NewGuid().ToString())"') DO ( SET NEWGUID=%a )
hadoop fs -mkdir wasb://nithinmhdi2@nithinwasb.blob.core.windows.net/output
hadoop fs -mkdir wasb://nithinmhdi2@nithinwasb.blob.core.windows.net/output/%NEWGUID%
hadoop fs -copyFromLocal -f beelineoutput.txt wasb://nithinmhdi2@nithinwasb.blob.core.windows.net/output/%NEWGUID%
hadoop fs -copyFromLocal -f log.txt wasb://nithinmhdi2@nithinwasb.blob.core.windows.net/output/%NEWGUID%