#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# For Other Debian Systems
# MYSQL_ARGS="--defaults-file=/etc/mysql/debian.cnf"

# For Home System
MYSQL_ARGS="--defaults-file=/etc/mysql/mysql.conf.d/mysqld.cnf"
DB="KennyInterview"
DELIM=","

# Create Database 
mysql -uroot -e "
CREATE DATABASE $DB;
"

# Load File One
CSV="$1"
TABLE=$(basename "$CSV" | cut -d. -f1) 


[ "$CSV" = "" -o "$TABLE" = "" ] && echo "Syntax: $0 csvfile tablename" && exit 1

FIELDS=$(head -1 "$CSV" | sed -e 's/'$DELIM'/` varchar(255),\n`/g' -e 's/\r//g')
FIELDS='`'"$FIELDS"'` varchar(255)'

mysql $MYSQL_ARGS $DB -e "
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE ($FIELDS);

LOAD DATA LOCAL INFILE '$CSV' INTO TABLE $TABLE
FIELDS TERMINATED BY '$DELIM'
IGNORE 1 LINES;
"


# Load File Two
CSV2="$2"
TABLE2=$(basename "$CSV2" | cut -d. -f1) 

[ "$CSV2" = "" -o "$TABLE2" = "" ] && echo "Syntax: $0 csvfile tablename" && exit 1

FIELDS=$(head -1 "$CSV2" | sed -e 's/'$DELIM'/` varchar(255),\n`/g' -e 's/\r//g')
FIELDS='`'"$FIELDS"'` varchar(255)'

#echo "$FIELDS" && exit

mysql $MYSQL_ARGS $DB -e "
DROP TABLE IF EXISTS $TABLE2;
CREATE TABLE $TABLE2 ($FIELDS);

LOAD DATA LOCAL INFILE '$CSV2' INTO TABLE $TABLE2
FIELDS TERMINATED BY '$DELIM'
IGNORE 1 LINES;
"

# Create Results Table
mysql $MYSQL_ARGS $DB -e "
DROP TABLE IF EXISTS RESULTS;
CREATE TABLE results ($FIELDS, flag varchar(20));
"

COLUMNONE=$(cut -d$DELIM -f1 $CSV | head -1)

# SELECT * FROM $TABLE NATURAL JOIN $TABLE2;
# SELECT * FROM $TABLE WHERE $TABLE.$COLUMNONE NOT IN (SELECT $COLUMNONE FROM $TABLE2);
# SELECT * FROM $TABLE2 WHERE $TABLE2.$COLUMNONE NOT IN (SELECT $COLUMNONE FROM $TABLE);
# SELECT * FROM $TABLE2 x WHERE EXISTS (SELECT 1 FROM $TABLE WHERE x.$COLUMNONE = $TABLE.$COLUMNONE AND NOT EXISTS (SELECT * FROM $TABLE y NATURAL JOIN $TABLE2 WHERE x.$COLUMNONE = y.$COLUMNONE));

mysql $MYSQL_ARGS $DB -e "
    INSERT INTO results
    SELECT *, 'Not Changed' AS flag FROM $TABLE NATURAL JOIN $TABLE2;

    INSERT INTO results
    SELECT *, 'Deleted' AS flag FROM $TABLE WHERE $TABLE.$COLUMNONE NOT IN (SELECT $COLUMNONE FROM $TABLE2);

    INSERT INTO results
    SELECT *, 'Added' AS flag FROM $TABLE2 WHERE $TABLE2.$COLUMNONE NOT IN (SELECT $COLUMNONE FROM $TABLE);

    INSERT INTO results
    SELECT *, 'Updated' AS flag FROM $TABLE2 x WHERE EXISTS (SELECT 1 FROM $TABLE WHERE x.$COLUMNONE = $TABLE.$COLUMNONE AND NOT EXISTS (SELECT * FROM $TABLE y NATURAL JOIN $TABLE2 WHERE x.$COLUMNONE = y.$COLUMNONE));

    SELECT * FROM results;
"
