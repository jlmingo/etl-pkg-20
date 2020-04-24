path_query = r'c:\Users\E353952\Desktop\ETL\Bypass\Plano de Contas EDP.csv'
path_sap = r'C:\Users\E353952\Desktop\ETL\SAP\2020\SAP Source'
path_sap_csv = r'c:\Users\E353952\Desktop\ETL\SAP\2020\SAP Source CSV'
path_packages = r'c:\Users\E353952\Desktop\ETL\Packages\2020'
path_scopes = r'c:\Users\E353952\Desktop\ETL\Scopes\2020'
path_trading_partner = r'c:\Users\E353952\Desktop\ETL\Join Tables\Trading_partner.xlsx'

scope_equivalences = {
    'EDPR-NA': "EDPR-NA",
    "EDPR-OF": "OF",
    "NEO-3": "NEO-3",
    "GR-EDP-RENOV": "EDPR",
    "EDPR-BR": "BR"
}
year = 2020

na_files = ["NA_2H19.csv", "EDPR-US_SAP_201901-06.csv", "EDPR-CA_SAP_201901-06.csv", "EDPR-MX_SAP_201901-06.csv"]