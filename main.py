"""script to create a range of delta lake metadata using apache spark"""
import os
import sys
import pwd
import grp
import random
import datetime
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import (LongType, StructType, StructField, StringType,
                               IntegerType, ArrayType, TimestampType, MapType)

EXTERNAL_PATH = sys.argv[1]
DB_NAME_ITER = sys.argv[2]
VOLUME_1000 = int(sys.argv[3])
VOLUME = VOLUME_1000 * 1000
PHONE_NUM = sys.argv[4].lower() == "true"
TIMESTAMP = sys.argv[5].lower() == "true"
ADDRESS_STRUCT = sys.argv[6].lower() == "true"

START_DATE = datetime.date(1980, 1, 1)
END_DATE = datetime.date(1990, 1, 1)
DATE_DIFF = (END_DATE - START_DATE).days
UPPER = string.ascii_uppercase
DIGITS = string.digits
spark = SparkSession.builder.getOrCreate()

structure = [
    StructField("first_name", StringType(), True),
    StructField("middle_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_day", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("age", IntegerType(), True)]

if PHONE_NUM:
    structure.append(StructField("phone_num", LongType(), True))

if TIMESTAMP:
    structure.append(StructField("entry_creation", TimestampType(), True))

if ADDRESS_STRUCT:
    structure.append(StructField("address", ArrayType(StructType([
        StructField("post_code", StringType(), True),
        StructField("street_address",
                    MapType(StringType(), IntegerType()),
                    True)]))))

schema = StructType(structure)

with open('./data/first-names.txt', encoding='utf-8') as fn_file:
    first_name = fn_file.read().split('\n')

with open('./data/middle-names.txt', encoding='utf-8') as mn_file:
    middle_name = mn_file.read().split('\n')

with open('./data/names.txt', encoding='utf-8') as ln_file:
    last_name = ln_file.read().split('\n')

with open('./data/streets.txt', encoding='utf-8') as st_file:
    streets = st_file.read().split('\n')


def random_num_phone():
    """generates random 11 digit integer beginning with 44"""
    phone_num = '44'
    for i in range(9):
        phone_num += str(random.randrange(i, i + 2))
    return int(phone_num)


def get_birth_day():
    """
    generates random date (between global start and end dates)
    to obtain birthday,month,year and age
    """
    birthday = START_DATE + datetime.timedelta(random.randrange(DATE_DIFF))
    day = int(birthday.day)
    month = int(birthday.month)
    year = int(birthday.year)
    age = int((START_DATE + (datetime.date.today() - birthday)
               ).year - START_DATE.year)
    return day, month, year, age


def get_address_struc():
    """
    generates array contain a random post code
    and map (containing street name and house number)
    """
    postcode = random.choice(UPPER) + random.choice(UPPER) + random.choice(
        DIGITS) + random.choice(DIGITS) + random.choice(UPPER) + random.choice(UPPER)
    street = random.choice(streets)
    house_num = int(random.randrange(1, 101))
    return postcode, street, house_num


def get_addition_string(_):
    """constructs the list of values used in an individual staging table insert statement"""
    person = {}

    day, month, year, age = get_birth_day()

    person["first_name"] = random.choice(first_name)
    person["middle_name"] = random.choice(middle_name)
    person["last_name"] = random.choice(last_name)
    person["birth_day"] = day
    person["birth_month"] = month
    person["birth_year"] = year
    person["age"] = age

    if PHONE_NUM:
        phone_num = random_num_phone()
        person["phone_num"] = phone_num
    if TIMESTAMP:
        person["entry_creation"] = datetime.datetime.now()
    if ADDRESS_STRUCT:
        postcode, street, house_num = get_address_struc()

        address = [{"post_code": postcode,
                    "street_address": {street: house_num}}]
        person["address"] = address
    return person


def search_and_replace():
    """replaces the DB name with the user defined one,
    then write the SQL script into the main directory
    under Hive user ownership"""
    with open('./TABLECREATE.sql', encoding='utf-8') as file:
        file_contents = file.read()

        updated_contents = file_contents.replace(
            "dbname", DB_NAME_ITER + ".db")

    with open(os.path.join(EXTERNAL_PATH, 'TABLECREATE.sql'), 'w', encoding='utf-8') as file:
        file.write(updated_contents)

    uid = pwd.getpwnam("hive").pw_uid
    gid = grp.getgrnam("hadoop").gr_gid
    os.chown(os.path.join(EXTERNAL_PATH, 'TABLECREATE.sql'), uid, gid)


def generate_hdfs_path():
    """Generating HDFS path for both table and database"""
    valid_path = os.path.join(EXTERNAL_PATH, "")
    hdfs_path = 'hdfs://' + valid_path + DB_NAME_ITER + '.db/'
    return hdfs_path


def create_external_path(table_name):
    """Generating the external path for the external tables"""
    hdfs_path_string = generate_hdfs_path()
    path_option = {'path': hdfs_path_string + table_name}
    return path_option


def create_data_frame():
    """
    creates a dataframe of the people data
    """

    itr_range = range(VOLUME)
    people = spark.sparkContext.parallelize(itr_range).map(
        get_addition_string)
    people_data_frame = spark.createDataFrame(people, schema)
    return people_data_frame


def data_frame_reader(table_name):
    """
    Reads a path for dataframes
    """

    path_option = create_external_path(table_name)
    existing_data_frame = spark.read.format('delta').load(**path_option)
    return existing_data_frame


def ingest_from_df_basic(table_name, df):
    """initiates basic delta table creation and ingests data from initial delta table"""

    path_option = create_external_path(table_name)
    df.write.mode('append').format("delta").save(**path_option)


def ingest_from_df_partition(table_name, partitions, df):
    """initiates partitioned delta table creation and ingests data from initial delta table"""

    path_option = create_external_path(table_name)
    df.write.mode('append').format("delta").partitionBy(
        partitions).save(**path_option)


def ingest_from_df_property(table_name, properties, df):
    """
    initiates delta table creation with
    table properties set and ingests
    data from initial delta table
    """

    path_option = create_external_path(table_name)
    path_prop = {**path_option, **properties}
    df.write.mode('append').format("delta").save(**path_prop)


def column_remove_table(df):
    """
    creates table and drops middle_name column from it,
    NOT IN USE DUE TO READER/WRITER VERSIONS AVAILABLE ON DELTA-HIVE CONNECTOR,
    KEPT IN CODE FOR POTENTIAL USE IN THE FUTURE
    """

    properties = {"delta.minReaderVersion": "2",
                  "delta.minWriterVersion": "5",
                  "delta.columnMapping.mode": "name"}
    drop_column_df = df.drop("middle_name")
    ingest_from_df_property("column_remove_table", properties, drop_column_df)


def union_table_create(table_name, table1, table2):
    """constructs and writes union table creation statement to an external file"""

    df1 = data_frame_reader(table1)
    df2 = data_frame_reader(table2)
    df3 = df1.union(df2)
    path_option = create_external_path(table_name)
    df3.write.mode('append').format("delta").save(**path_option)


def overwrite_insert_table(table_name, df):
    """creates table, truncates all its data and re-ingests the same data"""

    path_option = create_external_path(table_name)
    df.limit(0).write.mode('append').format('delta').save(**path_option)
    existing_data_frame = data_frame_reader(table_name)
    if existing_data_frame.isEmpty():
        df.write.mode('overwrite').format('delta').save(**path_option)
    else:
        df.write.mode('append').format('delta').save(**path_option)


def delete_insert_table(table_name, df):
    """
    Creates a table from data frame with limited rows,
    then will delete all data inside that table,
    then insert more rows to existing table
    """

    path_option = create_external_path(table_name)
    df.limit(0).write.mode('append').format('delta').save(**path_option)
    existing_data_frame = data_frame_reader(table_name)
    if existing_data_frame.isEmpty():
        df.limit(5).write.mode('overwrite').format(
            'delta').saveAsTable(table_name, **path_option)
        spark.sql("delete from " + table_name)
        df.write.mode('append').format('delta').save(**path_option)
        spark.sql("drop table " + table_name)
    else:
        df.write.mode('append').format('delta').save(**path_option)


def multi_partition_table(df):
    """creates tables with multiples nested partitions (birth_day,birth_month,birth_year)"""

    ingest_from_df_partition("multi_partition_table", [
        "birth_day", "birth_month", "birth_year"], df)


def datatype_partition_table(df):
    """
    creates tables partitioned by bigint (phone_num)
    and timestamp (entry_creation) columns respectively
    """
    if PHONE_NUM:
        ingest_from_df_partition("bigint_partition_table", ["phone_num"], df)
    if TIMESTAMP:
        ingest_from_df_partition(
            "timestamp_partition_table", ["entry_creation"], df)


def properties_table(df):
    """creates table with arbitrary table properties set"""

    properties = {'this.is.my.key': "14", 'this.is.my.key2"': "false"}
    ingest_from_df_property(
        "properties_table", properties, df)


def delta_properties_table(df):
    """creates table with delta specific table properties set"""
    properties = {"delta.appendOnly": "true",
                  "delta.logRetentionDuration": "interval 60 days"}
    ingest_from_df_property("delta_properties_table", properties, df)


def create_tables(df):
    """construct and writes sql used to create and populate all tables within an external file"""

    ingest_from_df_basic("initial_table", df)
    ingest_from_df_basic("initial_table_2", df)
    union_table_create("union_table",
                       "initial_table",
                       "initial_table_2")
    overwrite_insert_table("overwrite_insert_table", df)
    delete_insert_table("delete_rows_insert_table", df)
    multi_partition_table(df)
    datatype_partition_table(df)
    properties_table(df)
    delta_properties_table(df)


if __name__ == '__main__':
    db_location = generate_hdfs_path()
    spark.sql("create database if not exists " +
              DB_NAME_ITER +
              " location " +
              "'" + db_location + "'")
    spark.sql("use " + DB_NAME_ITER)

    data_frame = create_data_frame()
    create_tables(data_frame)

    spark.stop()
    search_and_replace()
