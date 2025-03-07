import logging

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""CREATE KEYSPACE IF NOT EXISTS spark_streams
                     WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
                    """)
    print("Keyspace created successfully")

def create_table(session):
    session.execute("""CREATE TABLE IF NOT EXISTS spark_streams.user_data
                     (title TEXT, 
                    first_name TEXT, 
                    last_name TEXT,
                    gender TEXT,
                    address TEXT,
                    postcode TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    timezone_offset TEXT,
                    timezone_description TEXT,
                    email TEXT,
                    username TEXT,
                    password TEXT,
                    salt TEXT,
                    uuid UUID PRIMARY KEY,
                    dob_date TEXT,
                    dob_age TEXT,
                    registered_date TEXT,
                    registered_age TEXT,
                    phone TEXT,
                    cell TEXT,
                    id_name TEXT,
                    id_value TEXT,
                    picture_large TEXT,
                    picture_medium TEXT,
                    picture_thumbnail TEXT,
                    nationality TEXT);
                    """)
    print("Table created successfully")


def insert_data(session, **kwargs):
    print("Inserting data into Cassandra")

    title = kwargs.get('title')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    latitude = kwargs.get('latitude')
    longitude = kwargs.get('longitude')
    timezone_offset = kwargs.get('timezone_offset')
    timezone_description = kwargs.get('timezone_description')
    email = kwargs.get('email')
    username = kwargs.get('username')
    password = kwargs.get('password')
    salt = kwargs.get('salt')
    uuid = kwargs.get('uuid')
    dob_date = kwargs.get('dob_date')
    dob_age = kwargs.get('dob_age')
    registered_date = kwargs.get('registered_date')
    registered_age = kwargs.get('registered_age')
    phone = kwargs.get('phone')
    cell = kwargs.get('cell')
    id_name = kwargs.get('id_name')
    id_value = kwargs.get('id_value')
    picture_large = kwargs.get('picture_large')
    picture_medium = kwargs.get('picture_medium')
    picture_thumbnail = kwargs.get('picture_thumbnail')
    nationality = kwargs.get('nationality')

    try:  
        session.execute("""
                        INSERT INTO spark_streams.user_data (title, first_name, last_name, gender, address, postcode, latitude, 
                        longitude, timezone_offset, timezone_description, email, username, password, salt, uuid, dob_date, 
                        dob_age, registered_date, registered_age, phone, cell, id_name, id_value, picture_large, picture_medium, 
                        picture_thumbnail, nationality)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (title, first_name, last_name, gender, address, postcode, latitude, 
                        longitude, timezone_offset, timezone_description, email, username, password, salt, uuid, dob_date, 
                        dob_age, registered_date, registered_age, phone, cell, id_name, id_value, picture_large, picture_medium, 
                        picture_thumbnail, nationality))
        logging.info(f"Data inserted successfully for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Error in inserting data: {e}")



def create_spark_connection():
    s_conn = None
# Go to Maven repository and search for the latest version of the spark-cassandra-connector. Here I found the latest version as 3.5.1 and Scala version 2.13.
# Similarly, search for the latest version of spark-sql-kafka-0-10. Here I found the latest version as 3.5.4 and Scala version 2.13.
    try:
        #s_conn = SparkSession.builder \
        #    .appName('SparkDataStreaming') \
        #    .config("spark.jars", "virt_env/lib/python3.10/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar") \
        #    .config("spark.jars", "virt_env/lib/python3.10/site-packages/pyspark/jars/spark-cassandra-connector_2.12-3.5.1.jar") \
        #    .config("spark.cassandra.connection.host", "localhost") \
        #    .getOrCreate()

        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           + "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info("Spark connection created successfully")
        
    except Exception as e:
        logging.error(f"Error in creating Spark connection: {e}")

    return s_conn
            
def connect_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users-data") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")

    except Exception as e:
        logging.error(f"Error in creating Kafka dataframe: {e}")

    return spark_df

def create_cassandra_connection():
# Connect to Cassandra cluster
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['cassandra'], port=9042, protocol_version=4, auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("Cassandra connection created successfully")
        return session

    except Exception as e:
        logging.error(f"Error in creating Cassandra connection: {e}")
        return None
    


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
                        StructField('title', StringType(), False),
                        StructField('first_name', StringType(), False),
                        StructField('last_name', StringType(), False),
                        StructField('gender', StringType(), False),
                        StructField('address', StringType(), False),
                        StructField('postcode', StringType(), False),
                        StructField('latitude', StringType(), False),
                        StructField('longitude', StringType(), False),
                        StructField('timezone_offset', StringType(), False),
                        StructField('timezone_description', StringType(), False),
                        StructField('email', StringType(), False),
                        StructField('username', StringType(), False),
                        StructField('password', StringType(), False),
                        StructField('salt', StringType(), False),
                        StructField('uuid', StringType(), False),
                        StructField('dob_date', StringType(), False),
                        StructField('dob_age', StringType(), False),
                        StructField('registered_date', StringType(), False),
                        StructField('registered_age', StringType(), False),
                        StructField('phone', StringType(), False),
                        StructField('cell', StringType(), False),
                        StructField('id_name', StringType(), False),
                        StructField('id_value', StringType(), False),
                        StructField('picture_large', StringType(), False),
                        StructField('picture_medium', StringType(), False),
                        StructField('picture_thumbnail', StringType(), False),
                        StructField('nationality', StringType(), False)
                        ])

    selection = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(selection)
    return selection

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka using Spark connection
        df = connect_kafka(spark_conn)
        cassandra_conn = create_cassandra_connection()

        if cassandra_conn is not None:
            create_keyspace(cassandra_conn)
            create_table(cassandra_conn)
            #insert_data(cassandra_conn, spark_conn)

            # Start the streaming
            logging.info("Starting the streaming process")
            selection_df = create_selection_df_from_kafka(df)
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                                            .option('checkpointLocation', '/tmp/checkpoint') \
                                            .option('keyspace', 'spark_streams') \
                                            .option('table', 'user_data') \
                                            .start())
            
            streaming_query.awaitTermination()
