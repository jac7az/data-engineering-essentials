import os
import duckdb

input_file = "https://s3.amazonaws.com/uvasds-systems/data/synthdata.parquet"

def clean_parquet():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='synthdata.duckdb', read_only=False)

        # Clear and import
        con.execute(f"""
            DROP TABLE IF EXISTS synthdata;
            CREATE TABLE synthdata
                AS
            SELECT * FROM read_parquet('{input_file}');
        """)
        ###Create Age column by calculating difference
        con.execute(f"""
            ALTER TABLE synthdata ADD COLUMN age INTEGER;
            UPDATE synthdata
            SET age=date_diff('year',birth_date,CURRENT_DATE);
                    """)
        print("created age column")
        ###Remove records w/ Null SCORE
        con.execute(f"""
                    DELETE FROM synthdata WHERE score IS NULL;
                    """)
        print("remove null")
         ###Remove duplicates
        con.execute(f"""
                    CREATE TABLE synthdata_clean AS SELECT DISTINCT * FROM synthdata;
                    DROP TABLE synthdata;
                    ALTER TABLE synthdata_clean RENAME TO synthdata;
                    """)
        print("remove duplicates")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()

