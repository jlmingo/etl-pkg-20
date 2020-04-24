import pandas as pd
import os

##ETL pipeline functions for packages

'''read package ytd for a certain month
'''
def read_YTD(path, month, dtype={"D_RU": str}):
    file = [file for file in os.listdir(path) if " 0"+str(month)+"M" in file][0]
    path_file = os.path.join(path,file)
    df = pd.read_csv(path_file, delimiter=";", dtype={"D_RU": str, "D_ORU": str, "D_T2": str})
    return df

'''read scope excel file for a certain month
'''
def read_scope(path,month):
    file = [file for file in os.listdir(path) if " "+str(month)+"M" in file][0]
    path_file = os.path.join(path,file)
    df = pd.read_excel(path_file)
    return df

'''Input: a dataframe from magnitude in standard format
Output: the dataframe treated. Ready to be used for further calculations.
'''
def transform_df(df):
    #Filter initial dataframe
    
    #Clients and products are null
    filter_clients_products = (df["D_CLIENTES"].isnull()) & (df["D_PRODUTOS"].isnull())
    
    #Active, passive and results accounts
    filter_2_1 = df["D_AC"].str.startswith("A")
    filter_2_2 = df["D_AC"].str.startswith("P")
    filter_2_3 = df["D_AC"].str.startswith("R")
    filter_accounts = (filter_2_1 | filter_2_2 | filter_2_3)
    
    #FL values start wuth F, except FA and FB which are dropped
    filter_3 = df["D_FL"].str.startswith("F")
    filter_4 = ~df["D_FL"].str.startswith("FA")
    filter_5 = ~df["D_FL"].str.startswith("FB")
    filter_flow = filter_3 & filter_4 & filter_5
    
    #Other nulls
    filter_6 = df["D_T1"] != "S9999"
    filter_7 = df["D_T2"].isnull()
    filter_8 = df["D_LE"].isnull()
    filter_9 = df["D_NU"].isnull()
    filter_10 = df["D_DEST"].isnull()
    filter_11 = df["D_AREA"].isnull()
    filter_12 = df["D_MU"].isnull()
    filter_13 = df["D_PMU"].isnull()
    filter_other_nulls = filter_6 & filter_7 & filter_8 & filter_9 & filter_10 & filter_11 & filter_12 & filter_13

    #Total filter
    filter_total = filter_clients_products & filter_accounts & filter_flow & filter_other_nulls
    df = df[filter_total]

    ##Drop rows
    #Drop results with F distinct from F99
    filter_1 = (df["D_AC"] == "P8800000") & (df["D_FL"] == "F10")
    filter_2 = ~df["D_AC"].str.startswith('R') & (df["D_FL"] == "F99")
    filter_3 = df["D_AC"].str.startswith('R') & (df["D_FL"] != "F99")
    index_drop = df[filter_1 | filter_2 | filter_3].index
    df = df.drop(index_drop)

    #Drop RU not beggining with "3"
    filter_ru = ~df["D_RU"].str.startswith("3")
    index_drop = df[filter_ru].index
    df = df.drop(index_drop)
    
    #columns selection
    selected_cols = ["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU", "D_T1", "P_AMOUNT"]
    df = df[selected_cols]

    #Passive and result multiplied by -1
    df.loc[filter_2_2, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)
    df.loc[filter_2_3, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)

    #Convert R, F99 to F10
    df.loc[df["D_AC"].str.startswith("R"), 'D_FL'] = "F10"

    ##fix transactions with third parties
    #separated dataframe with blank T1
    index_drop = df[~df["D_T1"].isnull()].index
    df_F_blanks = df.drop(index_drop)
    df_F_blanks.drop(["D_T1"], axis=1, inplace=True)

    #separated dataframe with "group companies" T1
    index_drop = df[df["D_T1"].isnull()].index
    df_F_ggcc = df.drop(index_drop)
    df_F_ggcc.drop(["D_T1"], axis=1, inplace=True)
    df_F_ggcc.loc[:,"P_AMOUNT"] = df_F_ggcc["P_AMOUNT"].multiply(-1)

    #additional dataframe with new calculated "S9999"
    df_third_parties = pd.concat([df_F_blanks, df_F_ggcc])
    df_third_parties = df_third_parties.groupby(["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU"], as_index=False).sum()
    df_third_parties.loc[:,"D_T1"] = "S9999"
    
    #output dataframe
    index_drop = df[df["D_T1"].isnull()].index
    df = df.drop(index_drop)
    df = pd.concat([df, df_third_parties])

    #type corrections
    df.loc[:,"D_RU"] = df["D_RU"].astype("str")
    df.loc[:,"D_PE"] = df["D_PE"].astype("datetime64[ns]")
    df.loc[:,"D_T1"] = df["D_T1"].astype("str")

    #clear rows with value 0
    df = df[df.P_AMOUNT != 0]
    
    #drop columns
    index_drop = ["D_CA", "D_DP", "D_PE"]
    df.drop(index_drop, axis=1, inplace=True)
    
    #rename columns
    df = df.rename(columns={"D_RU": "RU", "D_AC": "AC", "D_FL": "FL", "D_AU": "AU", "D_T1": "T1"})

    return df

'''given a transformed package dataframe, the scope dataframe, scope equivalences (to map exactly the scope name that must be input)
and the period, returns a dataframe with a new column including the scope
'''
def scope_adding(df, scope_df, scope_equivalences):
    scope_df = scope_df[["Reporting unit (code)", "Scope"]]
    scope_df.columns.values[0] = "RU"
    scope_df.loc[:,"RU"] = scope_df["RU"].astype("str")
    df = df.merge(scope_df, on="RU", how="left")
    df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")
    return df

'''given ytd of two consecutive months, it calculates a standalone month
'''
def ytd_to_month(df_YTD_current_month, df_YTD_previous_month):
    df_final_current_month = pd.concat([df_YTD_current_month, df_YTD_previous_month])
    df_final_current_month = df_final_current_month.groupby(['RU', 'AC', 'FL', 'AU', 'T1', 'Scope'], as_index=False).sum()
    return df_final_current_month


'''main transforming function for sap dataframes
'''
def transform_sap(df, df_join, path_scopes, path_trading_partner, scope_equivalences, file_name, max_months):
    
    #columns selection
    selection = ['Amount in local currency', 'Text', 'Trading partner', 'G/L Account',
    'Profit Center', 'Amount in doc. curr.', 'Order',
    'Year/month', 'Company Code', 'WBS element', 'Purchasing Document', 'Material',
    'General ledger amount', "Assignment", "Flow Type", "Document Date", "Document Number", 
    "Document type", "User Name", 'Account', "Aggregate Cost Center", "Asset",
    "Customer", "Vendor", "Document currency", "Document Header Text", "Entry Date", "Local Currency",
    "Posting Date", "Reference", "Reversed with"]
    df = df[selection]

    #drop rows where date contains month 13
    index_drop = df[df["Year/month"].str.contains("/13")].index
    df = df.drop(index_drop)      

    #format date column
    df.loc[:,"Year/month"] = pd.to_datetime(df["Year/month"])

    #correct numbers
    numeric_fields = ['Amount in local currency', 'Amount in doc. curr.', 'General ledger amount']
    df.loc[:, numeric_fields] = df[numeric_fields].replace(",", "", regex=True)
    df.loc[:,numeric_fields] = df[numeric_fields].astype("float")

    #change sign of amounts
    # df.loc[:,numeric_fields] = df[numeric_fields].multiply(-1)

    #add AU column
    df["AU"] = "0LIA01"
    
    #join_df to lookup AC
    df = df.merge(df_join, on="G/L Account", how="left")
    print(f"shape after merge: {df.shape}")

    #find new columns
    months_in_file = list(pd.to_datetime(df["Year/month"]).dt.month.unique())
    months_in_file = sorted(months_in_file, key=None, reverse=False)
    max_months_list = [x for x in range(1,max_months+1)]
    df_list_month = []
    for month in range(1, min(months_in_file[-1], max_months)+1):
        df_codes = df_codes_gen(path_scopes, month)
        df_month = df[df["Year/month"].dt.month == month]
        df_month = codes_columns_adding(df_month, df_codes)
        df_month = add_t1_cons_col(df_month, df_codes)
        df_list_month.append(df_month)
    df = pd.concat(df_list_month)

    #find new society code for Trading Partner
    # df_trading_partner = pd.read_excel(path_trading_partner, sheet_name="ZPMIG_ZCVBUND", dtype={"OLD CODE": str, "SIM R CODE": str})
    # df_trading_partner = df_trading_partner.rename(columns={"OLD CODE": "Trading partner", "SIM R CODE": "Reporting unit (code)"})
    # df_trading_partner = df_trading_partner.drop_duplicates(subset="Trading partner", keep="last")
    # df = df.merge(df_trading_partner[["Reporting unit (code)", "Trading partner"]], on="Trading partner", how="left")
    # df.loc[:,"Trading partner"] = df["Trading partner"].astype("str")
    df.loc[:, "Trading partner"] = df["Trading partner"].replace("nan", "S9999", regex=True)
    df["Trading partner"].fillna("S9999", inplace = True) 
    print(f"{df['Trading partner'].unique()}")
    # df.loc[:, "Reporting unit (code)_y"] = df["Reporting unit (code)_y"].replace("-", "S9999", regex=True)
    # df.drop("Trading partner", axis=1, inplace=True)
    df["FL"] = "F10"
    df = df.rename(columns={"Year/month": "PE",
                            "Trading partner": "T1",
                            "Company Code": "RU",
                            "Amount in local currency": "P_AMOUNT"})
    df = df.astype({'G/L Account': 'str', "T1": "str"})
    print(f"current shape: {df.shape}")
    
    #correct scopes
    df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")

    return df

def df_codes_gen(path_scopes, month):
    file_scope = [file for file in os.listdir(path_scopes) if " "+str(month)+"M" in file][0]
    df_codes = pd.read_excel(os.path.join(path_scopes, file_scope))
    return df_codes

def df_query_gen(path_query):
    df_join = pd.read_csv(path_query, dtype={"SAP_Local": "str"})
    df_join = df_join.rename(columns={"SAP_Local": "G/L Account", "SAP_CONS": "AC"}) 
    df_join = df_join[["G/L Account", "AC"]]
    return df_join

def add_t1_cons_col(df, df_codes):
    df_codes = df_codes.rename(columns={"Reporting unit (code)": "Trading partner", "Revised method (Closing)": "T1 Revised method (Closing)"})
    df_codes = df_codes.drop_duplicates(subset ="Trading partner", keep = "first")
    merging_columns = ["Trading partner", 'T1 Revised method (Closing)']
    df = df.merge(df_codes[merging_columns], on="Trading partner", how="left")
    df["T1 Revised method (Closing)"].fillna("External", inplace = True) 
    return df


def codes_columns_adding(df, df_codes):
    df_codes = df_codes.rename(columns={"Reporting unit (code)": "Company Code"})
    df_codes = df_codes.drop_duplicates(subset ="Company Code", keep = "first")
    merging_columns = ["Company Code", "Reporting unit (description)", 'Revised method (Closing)', 'Revised Conso. (Closing)',
    'Revised Own. Int. (Closing)', 'Revised Fin. Int. (Closing)', "Scope", "D_CU"]
    df = df.merge(df_codes[merging_columns], on="Company Code", how="left")
    print(f"shape after merge: {df.shape}")
    return df

def sap_dif_mag(df_pck, df_sap):
    #take only certain columns of df_sap and multiply by -1
    df_sap_2 = df_sap[['RU', 'AC', 'FL', 'AU', 'T1', 'P_AMOUNT', 'Scope', 'PE']].copy()
    df_sap_2.loc[:,"P_AMOUNT"] = df_sap_2['P_AMOUNT'].multiply(-1)

    #concat and groupby
    df_dif = pd.concat([df_pck, df_sap_2])
    df_dif = df_dif.groupby(['RU', 'AC', 'FL', 'AU', 'T1', 'Scope', 'PE'], as_index=False).sum()
    
    df_sap["Source"] = "SAP"
    df_dif["Source"] = "Differences"

    df_final = pd.concat([df_sap, df_dif])

    return df_final

def xlsx_to_csv(input_path, output_path):
    files_input = os.listdir(input_path)
    files_output = os.listdir(output_path)
    for file in files_input:
        file_name = str(file[:-4])
        if file_name+"csv" not in files_output:
            df = pd.read_excel(os.path.join(input_path, file))
            file_name = file_name+"csv"
            df.to_csv(os.path.join(output_path, file_name))
            print(str(file_name)+" created")

