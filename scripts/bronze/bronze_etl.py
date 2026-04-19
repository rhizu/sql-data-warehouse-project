/* =========================================================================
Script Purpose: This Script creates tables in the 'Bronze' schema, dropping 
existing tables, if they already exist.
Run this script to redefine the DDL structure of the 'Bronze' Tables.
============================================================================
/*

import os
import csv
import psycopg2
import time

#----------------------------------------------
# 1. CONFIG
#----------------------------------------------

BASE_PATH = "datasets"
CRM_PATH = os.path.join(BASE_PATH, "source_crm")
ERP_PATH = os.path.join(BASE_PATH, "source_erp")

#----------------------------------------------
# 2. DATABASE CONNECTION
#----------------------------------------------
try:
    if __name__ == "__main__":
        conn = psycopg2.connect(
            host = "localhost",
            database = "Data Warehouse",
            user = "postgres",
            password = "postgres"
        )
        print("Connection Successful!")
        cur = conn.cursor()

    #----------------------------------------------
    # 3. LOADER FUNCTION
    #----------------------------------------------
    start_time = time.time()

    def load_bronze(table_name, file_path, create_query):
        cur.execute(create_query)
        print(f"Table {table_name} created successfully!")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        print(f"Table {table_name} truncated successfully!")
        with open(f"{file_path}", "r") as file:
            cur.copy_expert(f"""
                    COPY {table_name}
                    FROM STDIN WITH CSV HEADER
    """, file)
        print(f"Data loaded successfully into {table_name}!")
    
        end_time = time.time()
        print(f"Loaded {file_path} in {end_time - start_time: .2f} seconds")
    #----------------------------------------------
    # 4. TABLE DEFINITIONS
    #----------------------------------------------

    create_crm_cust = """
    CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
        cst_id INT,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
                ); """

    create_crm_prd = """
    CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
        prd_id INT,
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    ); """

    create_crm_sales = """
    CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    ); """

    create_erp_loc = """
    CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
        CID VARCHAR(50),
        CNTRY VARCHAR(50)
    ); """

    create_erp_px = """
    CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
        ID VARCHAR(50),
        CAT VARCHAR(50),
        SUBCAT VARCHAR(50),
        MAINTENANCE VARCHAR(3)
    ); """

    create_erp_cust = """
    CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
        CID VARCHAR(50),
        BDATE DATE,
        GEN VARCHAR(50)
    ); """

    #----------------------------------------------
    # 5. LOAD CRM DATA
    #----------------------------------------------
    load_bronze(
        "bronze.crm_cust_info", 
        os.path.join(CRM_PATH, "cust_info.csv"),
        create_crm_cust
    )

    load_bronze(
        "bronze.crm_prd_info", 
        os.path.join(CRM_PATH, "prd_info.csv"),
        create_crm_sales
    )

    load_bronze(
        "bronze.crm_sales_details", 
        os.path.join(CRM_PATH, "sales_details.csv"),
        create_crm_prd
    )
    #----------------------------------------------
    # 6. LOAD ERP DATA
    #----------------------------------------------
    load_bronze(
        "bronze.erp_loc_a101", 
        os.path.join(ERP_PATH, "LOC_A101.csv"),
        create_erp_loc
    )

    load_bronze(
        "bronze.erp_px_cat_g1v2", 
        os.path.join(ERP_PATH, "PX_CAT_G1V2.csv"),
        create_erp_px
    )

    load_bronze(
        "bronze.erp_cust_az12", 
        os.path.join(ERP_PATH, "CUST_AZ12.csv"),
        create_erp_cust
    )

    #----------------------------------------------
    # 7. FINALIZE
    #----------------------------------------------

    conn.commit()

except Exception as e:
    print(f"Error occured: ", e)
    conn.rollback()

finally:
    conn.close()
    print("Connection closed")
