import duckdb

conn = duckdb.connect()
def main():
    # Connecting to memory
   
    
    # Creating Table
    conn.execute("""
        DROP TABLE IF EXISTS DuckDB;
        CREATE TABLE DuckDB AS 
        SELECT * FROM read_csv_auto('data/Electric_Vehicle_Population_Data.csv')
    """)
    
    # Altering the table name
    conn.execute("""ALTER TABLE DuckDB RENAME COLUMN "VIN (1-10)" TO VIN;
                ALTER TABLE DuckDB RENAME COLUMN "Postal Code" TO Postal_Code;
                ALTER TABLE DuckDB RENAME COLUMN "Model Year" TO Model_Year;
                ALTER TABLE DuckDB RENAME COLUMN "Electric Vehicle Type" TO Electric_Vehicle_Type;
                ALTER TABLE DuckDB RENAME COLUMN "Clean Alternative Fuel Vehicle (CAFV) Eligibility" TO Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility;
                ALTER TABLE DuckDB RENAME COLUMN "Electric Range" TO Electric_Range;
                ALTER TABLE DuckDB RENAME COLUMN "Base MSRP" TO Base_MSRP;
                ALTER TABLE DuckDB RENAME COLUMN "Legislative District" TO Legislative_District;
                ALTER TABLE DuckDB RENAME COLUMN "DOL Vehicle ID" TO DOL_Vehicle_ID;
                ALTER TABLE DuckDB RENAME COLUMN "Vehicle Location" TO Vehicle_Location;
                ALTER TABLE DuckDB RENAME COLUMN "Electric Utility" TO Electric_Utility;
                ALTER TABLE DuckDB RENAME COLUMN "2020 Census Tract" TO Census_2020_Tract;
                """)
    
    # Q1. Count the number of electric cars per city.
    def count_EVs_perCity():
        res = conn.execute("""
        SELECT City, COUNT('*') 
        FROM DuckDB 
        GROUP BY  City""").fetchall()

        print("Output Of Question 1")
        return res
    
    # Q2. Find the top 3 most popular electric vehicles.
    def top_3_most_Popular_EVs():
        res = conn.execute("""
        SELECT Make, Model, COUNT(Model) As Count_EV 
        FROM DuckDB 
        GROUP BY Make, Model 
        ORDER BY Count_EV DESC LIMIT 3""").fetchall()
        
        print("\nOutput Of Question 2")
        return res
    
    # Q3. Find the most popular electric vehicle in each postal code.
    def most_popular_EV_per_postalCode():
        res = conn.execute(""" 
        WITH Popular_ev_each_postal_code AS
        (SELECT Postal_Code, Make, Model, COUNT(*) As Cnt, ROW_NUMBER() OVER (PARTITION BY Postal_Code ORDER BY COUNT(*) DESC) As rn FROM DuckDB GROUP BY Postal_Code, Make, Model)
        SELECT Postal_Code, Make, Model FROM Popular_ev_each_postal_code WHERE rn=1;
         """  ).fetchall()

        print("\n Output Of Question 3")
        return res
    
    # Q4. Count the number of electric cars by model year. Write out the answer as parquet files partitioned by Model year.
    def count_of_Evs_by_model_year():
        conn.execute("""COPY (
          SELECT Model_Year, COUNT(*) AS count
          FROM DuckDB
          GROUP BY Model_Year
        )
        TO 'Result_Q4' 
        (FORMAT PARQUET, PARTITION_BY (Model_Year), OVERWRITE);""" ).fetchall()

        print("\nOutput Of Question 4")
        return "Files_Saved at Location Result_Q4/"


    print(count_EVs_perCity())
    print(top_3_most_Popular_EVs())
    print(most_popular_EV_per_postalCode())
    print(count_of_Evs_by_model_year())


if __name__ == "__main__":
    main()