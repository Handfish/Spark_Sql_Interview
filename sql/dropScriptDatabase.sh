#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# For Debian Systems
# MYSQL_ARGS="--defaults-file=/etc/mysql/debian.cnf"

# For Home System
MYSQL_ARGS="--defaults-file=/etc/mysql/mysql.conf.d/mysqld.cnf"
DB="KennyInterview"
DELIM=","

# Drop Database
mysql $MYSQL_ARGS $DB -e "
DROP DATABASE $DB
;
"
