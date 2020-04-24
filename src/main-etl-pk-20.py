import pandas as pd
import os
from variables import *
from functions import *

'''
TO-DO create a log report to check if all files are in folders
'''
def main():

    program_mode = int(input("Please select program mode: ")) #0: both pck and SAP; 1: only pck; 2: only SAP
    max_months = int(input("Please select latest month to run the program: "))
    list_df_months = []

    if program_mode in [0,1]:
        #the code below generates a file for the packages
        print("STEP 1 / 2 - Generating packages file")
        for month in range(1, max_months+1):    
            
            period = str(year)+str(month).zfill(2)+"01"

            if month == 1:
                df_current_month = transform_df(read_YTD(path_packages, month))
                df_scope = read_scope(path_scopes, month)
                df_current_month = scope_adding(df_current_month, df_scope, scope_equivalences)
                df_current_month["PE"] = pd.to_datetime(period, format='%Y%m%d')
                df_current_month.loc[df_current_month["PE"] == "2020-01-01","P_AMOUNT"] = 0
                list_df_months.append(df_current_month)
                df_previous_month = df_current_month.copy()
                index_drop = ["PE"]
                df_previous_month.drop(index_drop, axis=1, inplace=True)
                print(f"{df_current_month.P_AMOUNT.sum()}")
                print(f'**Month {month} correctly processed**')
            else:
                for i in range(month-1,month+1):
                    if i == month:
                        df_current_month = transform_df(read_YTD(path_packages, month))
                        df_scope = read_scope(path_scopes, month)
                        df_current_month = scope_adding(df_current_month, df_scope, scope_equivalences)
                    else:
                        df_previous_month.loc[:,"P_AMOUNT"] = df_previous_month["P_AMOUNT"].multiply(-1)
                
                df_month = ytd_to_month(df_current_month, df_previous_month)
                df_month["PE"] = pd.to_datetime(period, format='%Y%m%d')
                print(f"appending month {month}")
                print(f"{df_month.P_AMOUNT.sum()}")
                df_month.to_csv("../output/prueba.csv")
                list_df_months.append(df_month)
                df_previous_month = df_current_month.copy()
                print(f'**Month {month} correctly processed**')
            
        df_pck = pd.concat(list_df_months)
        df_pck.loc[:,"P_AMOUNT"] = df_pck["P_AMOUNT"].round(2)
        df_pck = df_pck[df_pck["P_AMOUNT"] != 0]
        df_pck.loc[df_pck["PE"] == "2020-01-01","P_AMOUNT"] = 0
        df_pck.to_csv(f"../output/monthly_pl&bs_pk_{year}.csv", index=False)
        print(f"CSV correctly generated as output/monthly_pl&bs_pk_{year}.csv")

    if program_mode in [0,2]:
        #the code below generates a file for SAP
        print("STEP 2 / 2 - Generating SAP file")
        
        list_df_sap = []
        df_join = df_query_gen(path_query)
        xlsx_to_csv(path_sap, path_sap_csv)
        files_sap = [file for file in os.listdir(path_sap_csv) if "csv" in file]
        for file in files_sap:
            print(f"processinng {file}")
            df = pd.read_csv(os.path.join(path_sap_csv, file), dtype={"G/L Account": "str", 
                                                                    "Trading partner": "str", "Company Code": "str", "Document Date": "str", 
                                                                    "Document Number": "str", "Document type": "str", "Account": "str", 
                                                                    "User Name": "str", 'Account': "str", "Aggregate Cost Center": "str", "Asset": "str",
                                                                    "Customer": "str", "Vendor": "str", "Document currency": "str", "Document Header Text": "str", 
                                                                    "Entry Date": "str", "Local Currency": "str",
                                                                    "Posting Date": "str", "Reference": "str", "Reversed with": "str"})
            df.loc[df["Trading partner"].isnull() == False,"Trading partner"] = df["Trading partner"].str.replace(".0", "", regex=False)
            df = transform_sap(df, df_join, path_scopes, path_trading_partner, scope_equivalences, file, max_months)
            list_df_sap.append(df)
        df_final_sap = pd.concat(list_df_sap)
        print("Generating sap csv...")
        df_final_sap.to_csv(f"../output/sap_pl_{year}.csv", index=False)
        print(f"CSV correctly generated as output/sap_pl_{year}.csv")

    if (program_mode == 0):
        df_sap_dif = sap_dif_mag(df_pck, df_final_sap)
        df_sap_dif.to_csv(f"../output/monthly_pl&bs_pk&sap_{year}.csv", index=False)
        print(f"CSV correctly generated as output/monthly_pl&bs_pk&sap_{year}.csv")

if __name__ == "__main__":
    main()