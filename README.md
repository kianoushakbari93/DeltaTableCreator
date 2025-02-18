**Delta Lake Table Generator**

**_Overview_**

These scripts are designed to create medium-to-large scale Delta Lake tables (up to 100,000 entries) within Hive. As this table format is not innately supported on Hive, a Hive-Delta connector jar is employed. This has meant that all tables created on Hive by these scripts are required to be non-editable. For these reasons another objective when creating the Delta tables is to achieve wide reaching test coverage, through partitions, table properties and pre-performed table operations.

These scripts have been created in accordance with UAT-1377 in order to effectively test source-side Delta Lake migration.

**_Tables created_**

This set of scripts creates the following tables:

- **initial_table:** standard Delta Lake table created from ingest data
- **initial_table_2:** standard Delta Lake table created from ingest data
- **union_table:** standard Delta Lake table created from union of initial tables
- **delete_rows_insert_table:** identical table to initial_table with the ingested data deleted before being inserted again
- **overwrite_insert_table::** identical table to initial_table with the ingested data being overwritten to the existing table
- **multi_partition_table:** table partitioned on birth day, month and year
- **bigint_partition_table:** table partitioned on bigint phone number values
- **timestamp_partition_table:** table partitioned on timestamp entry creation values
- **properties_table:** table with arbitrary table properties set
- **delta_properties_table:** table with Delta-specific table properties set

Delta tables created by this script contain the following fields:

- **first_name:** string
- **middle_name:** string
- **last_name:** string
- **birth_day:** int
- **birth_month:** int
- **birth_year:** int
- **age:** int
- **phone_num:** bigint
- **entry_creation:** timestamp
- **address:** array<struct<post_code:string,street_address:map<string,int>>>

**_Configurable parameters_**

**In start.sh:**

- HIVE_METASTORE_VERSION: The version of Hive on cluster. HINT: Run the "hive --version" on any VM
- HIVE_METASTORE_URIS: The thrift URI for HMS(Hive metastore). Search hive.metastore.uris in the hive-site.xml -> Usually HMS thrift is on vm0 on port 9083. Example: thrift://clusterx-vm0.bdauto.wandisco.com:9083
- HIVE_PRINCIPAL: Hive kerberos principal
- HIVE_KEYTAB: Location of Hive kerberos keytab
- HDFS_PRINCIPAL: HDFS kerberos principal
- HDFS_KEYTAB: Location of HDFS kerberos keytab
- SPARK_DL_LINK: Link to download the related spark-3.5.x-bin-hadoop3-scala2.13.tgz from this link -> https://spark.apache.org/downloads.html
- DB_NAME: Name of database created by scripts (must be unique each time the script is run)
- HIVE_CONNECTOR_DL_LINK: Link to download the delta hive connector JAR file -> https://github.com/delta-io/delta/releases/download/v3.2.0/delta-hive-assembly_2.13-3.2.0.zip
- EXTERNAL_PATH: Location to store the table's data
- VOLUME_1000: number of entries in each table multiplied to 1000
- BIGINT: boolean to determine if the user wants the phone number bigint field to be used and bigint_partition_table to be created
- TIMESTAMP: boolean to determine if the user wants the entry creation timestamp field to be used and timestamp_partition_table to be created
- STRUCT: boolean to determine if the user wants the address array field to be used
- ITERATIONS: number of times to run the script to create n number of databases

**_Pre-configurations on Cluster_**

- On **VM0** run the commands bellow, to download the Delta Hive Assembly archiv(delta-hive-assembly\*2.13-3.2.0.zip) and put it in this path → /opt/hive-aux-jars/

- _mkdir /opt/hive-aux-jars_

- _wget https://github.com/delta-io/delta/releases/download/v3.2.0/delta-hive-assembly_2.13-3.2.0.zip_

- _unzip delta-hive-assembly_2.13-3.2.0.zip_

- _cp delta-hive-assembly\*\_.jar /opt/hive-aux-jars_

- _rm -rf delta-hive-assembly\_\_ \_\_MACOSX/_

- _chown -R hive:hive /opt/hive-aux-jars/_

**On HDP:**

- Add these properties to the Custom hive-site in the cluster manager to Hive: Cluster manager → Hive → ADVANCED → Custom hive-site

    - _hive.aux.jars.path=file:///opt/hive-aux-jars/delta-hive-assembly_2.13-3.1.0.jar,<comma-separate-if-you-have-any-other-jar.jar>_

    - _hive.input.format=io.delta.hive.HiveInputFormat_

    - _hive.security.authorization.sqlstd.confwhitelist.append=hive\.input\.format|<pipe-will-separate-the-values>_


- Search and change the hive.tez.input.format to io.delta.hive.HiveInputFormat inside the Hive: Cluster manager → Hive → ADVANCED → Advanced hive-site

- Search and find "Auxiliary JAR list" in the Hive and set the value to the Delta uber jar file path: Cluster manager → Hive → ADVANCED

- Restart the required services to take effect.

**On CDP:**

- Add these 4 properties to the hive-site.xml through the hive-on-tez1 in the cluster manager. Cluster manager → hive-on-tez1 → Configuration → search for "Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml"
    - hive.security.authorization.sqlstd.confwhitelist.append=hive\.input\.format
    - hive.tez.input.format=io.delta.hive.HiveInputFormat
    - hive.input.format=io.delta.hive.HiveInputFormat
    - hive.aux.jars.path=file:///opt/hive-aux-jars/delta-hive-assembly_2.13-3.1.0.jar

- Search for Hive Auxiliary JARs Directory in the hive-on-tez1 in the cluster and set the value to the path for directory that contain the Delta uber JAR file: Cluster manager → hive-on-tez1 → Configuration → search for "Hive Auxiliary JARs Directory" and add this path → "/opt/hive-aux-jars/"

- Restart the required services to take effect.

_How to run_

**Become sudo (root) user**

- su --

**Install Python3**

- e.g. on CentOS 7: yum install python3

**Make start script executable**

- chmod +x start.sh

**Run start script**

- ./start.sh

The scripts were originally going to create a further table which had one of its columns dropped. However the Hive-Delta connector version 3.2.0 jar only has Delta reader 1 and writer 2 available to it, whilst column operations are only possible on reader 2 and writer 5 or above. The code to create this table still exists in main.py to potentially be re-incorporated when the connector supports these versions.

If the scripts are run on a CDH machine more than once it will cause the connector jar to be temporarily loaded incorrectly. This is a CDH related issue that cannot be resolved currently. It does NOT impact the creation and population of the Delta tables, it does stop the Delta data from being queried (i.e. SELECT _ FROM) however the Delta tables themselves can still be queried in this period (i.e. SELECT COUNT(_) FROM). If left for approximately 3 hours this issue does resolve itself.

**_Process Flow_**

**BASH**

- Copy the ALTERDROP.sql script in the staging directory for later to run in Hive
- Download and install Spark if it doesn’t already exist
- Delete spark tgz
- Download a few JAR files from Maven repository for the Spark to work with Hive
- Download and put the Delta Hive Assembly JAR file in the /opt/hive-aux-jars on the current VM
- Put the Hadoop XML files in the Spark config path
- Create a /delta directory in the HDFS and give the ownership to Hive
- Upgrade pip3
- Pip install required packages
- Run Spark-submit with the specified configs to connect to Hive and run the Python script to create the tables

**PYTHON**

- Build Spark session
- Create and use the DB
- Create initial Delta table from the people dataframe
- Create truncate Delta table and ingest contents of initial table into it
- Truncate table
- Ingest contents of initial table into truncate table
- Create multi level partitioned Delta directory and table
- Ingest contents of people data frame into multi level partitioned Delta table
- Create bigint partitioned table (if set by user)
- Ingest contents of people data frame into bigint partitioned Delta table
- Create timestamp partitioned Delta directory and table (if set by user)
- Ingest contents of people data frame into timestamp partitioned Delta table
- Create properties-set Delta directory and table
- Ingest contents of people data frame into properties-set Delta table
- Create delta specific properties-set Delta table
- Ingest contents of people data frame into Delta specific properties-set Delta table
- Stop Spark session
- Perform Database naming replacement on the TABLECREATE.sql
- Place the TABLECREATE.sql inside the main directory on local
- Perform hive:hadoop chown on TABLECREATE.sql script created

**BASH**

- Kinit as HDFS user
- Delete extra directory for the DB and staging table
- Kinit as Hive user
- Enter beeline (through hive)
- Use the created database
- ALTER & DROP the Delta tables that are not working with the Hive by running the ALTERDROP.sql
- Run table creation SQL script (created previously)
- Show tables created
- Destroy kerberos details
- Delete locally stored SQL scripts
