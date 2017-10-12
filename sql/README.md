## SQL Interview Application
To run `./createDbAndTables.sh 1.csv 2.csv`
To clean `./dropScriptDatabase.sh`

https://stackoverflow.com/questions/9998596/create-mysql-table-directly-from-csv-file-using-the-csv-storage-engine
Seeing as it is not possible to create a table schema from a csv directly, I made everything a varchar(255)

Wasn't sure what database we were working in but I completed this with MYSQL

MYSQL is not as powerful as other sql languages. It lacks functionality such as INTERSECT and MINUS, which would have made this much easier.
