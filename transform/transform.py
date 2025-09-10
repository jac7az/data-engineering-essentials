import os
import duckdb

input_file = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
local_parquet='output.parquet'

"""_summary_
0. set up duckdb connection
1. drop table if it exists
2. read_parquet() in as a new table
3. count the number of record in the new table
4. basic cleaning
5. save new table as a local parquet file
"""

def duckdb_read_parquet(input_file):

    con = None  #No value, dimensions or parameters

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='transform.duckdb', read_only=False)

        #clear out table if it exists
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_tripdata_202501;
        """)
        print("Table has been dropped")

        con.execute(f"""
        CREATE TABLE yellow_tripdata_202501 AS SELECT * FROM read_parquet('{input_file}');   --no double quotes-- 
        """)
        print("Table as been created")

        count=con.execute(f"""
        SELECT COUNT(*) FROM yellow_tripdata_202501;
""")
        print(f"Number of record: {count.fetchone()[0]}")

        con.execute(f"""
        CREATE TABLE yellow_tripdata_202501_clean AS SELECT DISTINCT * FROM yellow_tripdata_202501;
        DROP TABLE yellow_tripdata_202501;
        ALTER TABLE yellow_tripdata_202501_clean RENAME TO yellow_tripdata_202501;
""")
        print("Clean table has been created")

        con.execute(f"""
        COPY yellow_tripdata_202501 TO '{local_parquet}' (FORMAT PARQUET);
""")
        print("Table copied to local parquet.")

        con.execute(f"""
        ATTACH '' AS rds (TYPE mysql, SECRET rds);  --ATTACH is unique duckdb command to connect to rds database--
""")
        con.execute(f"""
        DROP TABLE IF EXISTS rds.yellow_tripdata_202501;
        CREATE TABLE rds.yellow_tripdata_202501 AS 
            SELECT * FROM transform.yellow_tripdata_202501 LIMIT 100000;  --local database transform to network database rds--     
""")
        print("Connection successful")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":          #main handler: actually handles the code and by default run the function
    duckdb_read_parquet(input_file)
