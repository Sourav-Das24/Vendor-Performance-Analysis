import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary 
       and add new columns in the result data'''
    
    vendor_sales_summary = pd.read_sql_query("""
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,   
            
            -- Purchases
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars,
            
            -- Freight
            f.FreightCost,
            
            -- Sales
            s.TotalSalesDollars,
            s.TotalSalesPrice,
            s.TotalSalesQuantity,
            s.TotalExciseTax,
            
            -- Derived column
            (IFNULL(s.TotalSalesDollars,0) 
                - IFNULL(SUM(p.Dollars),0) 
                - IFNULL(f.FreightCost,0) 
                - IFNULL(s.TotalExciseTax,0)) AS Profit
        
        FROM purchases p
        JOIN purchase_prices pp 
            ON p.Brand = pp.Brand
        
        LEFT JOIN (
            SELECT VendorNumber, SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ) f 
            ON p.VendorNumber = f.VendorNumber
        
        LEFT JOIN (
            SELECT VendorNo, Brand, 
                   SUM(SalesDollars) AS TotalSalesDollars,
                   SUM(SalesPrice) AS TotalSalesPrice,
                   SUM(SalesQuantity) AS TotalSalesQuantity,
                   SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        ) s 
            ON p.VendorNumber = s.VendorNo 
            AND p.Brand = s.Brand
        
        WHERE p.PurchasePrice > 0
        
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, 
                 p.PurchasePrice, pp.Volume, pp.Price, f.FreightCost, 
                 s.TotalSalesDollars, s.TotalSalesPrice, 
                 s.TotalSalesQuantity, s.TotalExciseTax
        
        ORDER BY TotalPurchaseDollars DESC
    """, conn)
    
    return vendor_sales_summary


def clean_data(df):
    '''This Function will clean the data'''
    # Changing data type to float
    df['Volume'] = df['Volume'].astype('float64')

    # Filling missing values with 0
    df.fillna(0, inplace = True)

    # Removing leading/trailing spaces in VendorName
    df['VendorName'] = df['VendorName'].astype(str).str.strip()

    # Creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] /  df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']

    return df
    
if __name__ == '__main__':
    # Creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')
