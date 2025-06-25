#%% Get Russell 3000 stock tickers

import pandas as pd

def get_russell_3000_tickers():
    """
    Downloads the Russell 3000 ETF (IWV) holdings CSV from iShares
    and returns the full list of tickers (~3,000).
    """
    # iShares Russell 3000 ETF (IWV) holdings CSV endpoint
    url = (
        "https://www.ishares.com/us/products/239714/ishares-russell-3000"
        "/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    )
    try:
        # Read CSV directly
        df = pd.read_csv(url, skiprows=9)
        tickers = df["Ticker"].dropna().unique().tolist()
        print(f"✅ Retrieved {len(tickers)} tickers (expected ~3000).")
        return tickers
    except Exception as e:
        print("⚠️ Error fetching tickers:", e)
        return []

if __name__ == "__main__":
    tickers_R3000 = get_russell_3000_tickers()
    print(tickers_R3000[:10])
    

#%% Testing AlphaVantageData DataAccessor & FeatureEngineer June 11


"""Example data pipeline using AlphaVantageData, DatabaseAccessor and FeatureEngineer.

The script fetches various data from Alpha Vantage, stores it in an SQLite
database, then reads the data back and computes sample features. The final
result is made available as a pandas DataFrame for further analysis.
"""


import os
from typing import List
import pandas as pd

from config import AV_api_key, Data_dir #, AV_db_file
from classes import DataFetcher, DatabaseAccessor, FeatureEngineer
# SQLite database file used to store retrieved data
AV_db_file = os.path.join(Data_dir, "av_data_test1.db") #amending the config file's location

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Placeholder list of tickers to fetch data for
#TICKERS: List[str] = ["AAPL", "MSFT", "GOOGL"]  # e.g. ["AAPL", "MSFT", "GOOGL"]
TICKERS: List[str] = ['NVDA', 'COKE', 'IAU', 'BRK-B', 'TSLA', 'LAMR', 'MSFT', 'SOFI', 'AAPL', 'ZS', 'JPM', 'GS', 'AXP', 'VICI', 'AVGO', 'AMZN', 'META', 'BRK-B', 'GOOGL', 'CSCO', 'GS', 'GOOG', 'UNH', 'V', 'CRM', 'PANW', 'HD', 'COST', 'CRWD', 'IBM', 'NOTE', 'LLY', 'FTNT', 'AMGN', 'NFLX', 'PNC', 'USB', 'SHW', 'CAT', 'HON', 'WMT', 'MA', 'PLTR', 'NET', 'GD', 'NOC', 'ADBE', 'AMD', 'ORCL', 'TFC', 'TRV', 'INTU', 'QCOM', 'TXN', 'ANET', 'MCD', 'NEE', 'XOM', 'NOW', 'BAC', 'ADI', 'C', 'AMAT', 'FFIV', 'PGR', 'MU', 'ACN', 'OKTA', 'BA', 'TMUS', 'LDOS', 'KLAC', 'LRCX', 'CEG', 'WFC', 'GEN', 'CHKP', 'CYBR', 'RBRK', 'BLK', 'INTC', 'BAH', 'CDNS', 'BX', 'KKR', 'MMM', 'SNPS', 'MRVL', 'SCHW', 'MTB', 'CB', 'APP', 'APH', 'QLYS', 'CMCSA', 'SO', 'PG', 'TENB', 'MMC', 'VRNS', 'ADSK', 'S', 'MSI', 'DUK', 'CVX', 'FITB', 'SPGI', 'COP', 'VRTX', 'ICE', 'MS', 'JNJ', 'ADP', 'WDAY', 'ROP', 'HBAN', 'GILD', 'NXPI', 'ISRG', 'PYPL', 'RPD', 'FSLY', 'FANG', 'LIN', 'SBUX', 'FCNCA', 'FI', 'AJG', 'AEP', 'PEP', 'CME', 'VST', 'RF', 'NKE', 'ATEN', 'APO', 'EOG', 'CFG', 'BSX', 'MCO', 'BK', 'MSTR', 'AON', 'DIS', 'ETN', 'FICO', 'TJX', 'CTAS', 'REGN', 'SRE', 'EXC', 'EQT', 'AFL', 'TEAM', 'XEL', 'BKNG', 'OXY', 'MRK', 'KEY', 'DDOG', 'MDLZ', 'PEG', 'ALL', 'AMP', 'D', 'IT', 'MAR', 'HESM', 'TEL', 'ORLY', 'DVN', 'CTRA', 'DELL', 'ANSS', 'HPE', 'WES', 'LOW', 'PFE', 'UBER', 'UNP', 'GLW', 'AIG', 'MCHP', 'CTSH', 'PSTG', 'PCG', 'MET', 'PRU', 'DFS', 'SYK', 'NTAP', 'CSX', 'MSCI', 'EXE', 'GDDY', 'TT', 'EWBC', 'ON', 'SNOW', 'HUBS', 'APA', 'AR', 'PCAR', 'ABNB', 'AXON', 'GEV', 'DE', 'CDW', 'ETR', 'COF', 'HPQ', 'MDB', 'MDT', 'BMY', 'NTNX', 'ROST', 'PH', 'KEYS', 'PR', 'CPRT', 'MCK', 'GE', 'MPWR', 'KO', 'PAYX', 'FGXXX', 'PLD', 'ED', 'TDG', 'HIG', 'WMB', 'WEC', 'OVV', 'WELL', 'AKAM', 'BKR', 'TYL', 'LMT', 'MELI', 'NDAQ', 'CL', 'FHN', 'COIN', 'WTW', 'CMG', 'WM', 'MTDR', 'ACGL', 'UPS', 'RRC', 'CHRD', 'EIX', 'CHTR', 'FAST', 'ELV', 'TDY', 'TWLO', 'HLT', 'WBS', 'OKE', 'MNST', 'LULU', 'VRSK', 'WAL', 'KMI', 'DTE', 'MO', 'EQIX', 'AMT', 'PTC', 'RJF', 'AWK', 'WDC', 'PPL', 'AEE', 'KDP', 'ZBRA', 'HWM', 'STT', 'CI', 'EA', 'BRO', 'NRG', 'WTFC', 'TER', 'TRMB', 'SM', 'PNFP', 'IDXX', 'CFR', 'RCL', 'TTD', 'STX', 'NFG', 'TRGP', 'ECL', 'VRSN', 'WDS', 'DXCM', 'ZTS', 'JBL', 'ZION', 'CMA', 'CIVI', 'DASH', 'PDD', 'URI', 'FE', 'DOCU', 'SSB', 'ATO', 'ASML', 'FSLR', 'GWW', 'ZM', 'WK', 'FIS', 'APD', 'EMR', 'ES', 'TROW', 'CBSH', 'TTWO', 'CMS', 'MUR', 'SNV', 'VZ', 'CNP', 'CSGP', 'BR', 'BDX', 'ITW', 'CVS', 'PB', 'GTLB', 'CNX', 'MGY', 'KHC', 'DLR', 'CBOE', 'EPAM', 'BPOP', 'ARES', 'TGT', 'FDX', 'SWKS', 'CFLT', 'NOG', 'CRGY', 'PSX', 'COR', 'CINF', 'MPC', 'SPG', 'NTRS', 'PWR', 'RSG', 'CMI', 'SMCI', 'T', 'NI', 'IR', 'FCX', 'GM', 'HCA', 'NSC', 'AZO', 'JCI', 'SLB', 'CARR', 'MANH', 'DT', 'JNPR', 'RTX', 'CRC', 'CRDO', 'FLEX', 'VRN', 'SYF', 'LNT', 'IRM', 'WRB', 'O', 'HOOD', 'WAB', 'CIEN', 'DBX', 'FNB', 'GRMN', 'EVRG', 'IONQ', 'GPOR', 'ENTG', 'PFG', 'GWRE', 'L', 'COHR', 'FDS', 'CRK', 'DAL', 'CTVA', 'AME', 'CBRE', 'A', 'FIVN', 'DHI', 'LHX', 'HES', 'OZK', 'KR', 'KVUE', 'GEHC', 'BIIB', 'NEM', 'PSA', 'LPLA', 'DECK', 'GPN', 'NU', 'GBCI', 'KD', 'APPF', 'BTE', 'MLM', 'GEHC', 'ODFL', 'AZN', 'EG', 'ESTC', 'SOUN', 'QTWO', 'HOMB', 'CTSH', 'CPAY', 'ENPH', 'LSCC', 'CCI', 'F', 'EW', 'VLO', 'KMB', 'U', 'OTIS', 'IQV', 'VMC', 'ODFL', 'UMBF', 'MKL', 'YUM', 'RMD', 'SYY', 'UAL', 'HUM', 'EFX', 'VTLE', 'VLTO', 'UBSI', 'RNG', 'BOX', 'CCEP', 'SNX', 'BILL', 'MTSI', 'AZPN', 'LUMN', 'DOCN', 'PCOR', 'IOT', 'AUR', 'SMAR', 'ALTR', 'FFIN', 'PHM', 'KOS', 'IBKR', 'GIS', 'ROK', 'DD', 'VLY', 'NVR', 'CNC', 'EXR', 'VICI', 'ONTO', 'EBAY', 'CAH', 'TSCO', 'AVB', 'LEN', 'DOW', 'NUE', 'XYL', 'PPG', 'VNT', 'PATH', 'PNW', 'RMBS', 'IDCC', 'BSY', 'BMI', 'SPSC', 'FN', 'LITE', 'SMTC', 'CVLT', 'XYZ', 'BL', 'AES', 'BLKB', 'SOFI', 'SHOP', 'MKSI', 'OLED', 'ARW', 'PKG', 'VET', 'SW', 'CWAN', 'AI', 'DLB', 'LFUS', 'HCP', 'SANM', 'PLXS', 'CGNX', 'ITRI', 'AVT', 'ACIW', 'SITM', 'AEIS', 'SLAB', 'CCCS', 'BDC', 'PEGA', 'AIZ', 'GL', 'MTD', 'FTV', 'DOV', 'TPL', 'EQR', 'CCL', 'HAL', 'CHD', 'WAT', 'VTR', 'HUBB', 'WST', 'ADM', 'KVYO', 'FNF', 'QRVO', 'NOVT', 'POWI', 'DXC', 'MARA', 'INTA', 'AGYS', 'MIR', 'ASGN', 'RGA', 'ASAN', 'TW', 'WBD', 'GFS', 'VTS', 'STZ', 'SD', 'MKTX', 'EQH', 'INFXX', 'RNR', 'JEF', 'SPT', 'WBD', 'HSY', 'SBAC', 'IFF', 'DRI', 'LYV', 'WY', 'EXPE', 'ZBH', 'DIOD', 'CRUS', 'FRSH', 'VERX', 'ALRM', 'SYNA', 'AMBA', 'CXT', 'NCNO', 'RIOT', 'NSIT', 'PI', 'FORM', 'TDC', 'ZETA', 'LH', 'K', 'STE', 'LII', 'WTRG', 'LYB', 'IP', 'ULTA', 'PODD', 'CLX', 'LUV', 'SNA', 'BLDR', 'COO', 'MKC', 'OWL', 'SAP', 'BOKF', 'UNM', 'IVZ', 'CG', 'WIX', 'ARM', 'PAYC', 'EL', 'SF', 'FROG', 'PAR', 'KLIC', 'AMKR', 'PRGS', 'DV', 'BRZE', 'ALGM', 'TTMI', 'OSIS', 'AGPXX', 'OBE', 'OGE', 'NLY', 'APPN', 'TOST', 'JKHY', 'ALLY', 'EVR', 'PRI', 'REPX', 'KNSL', 'ESS', 'BBY', 'STLD', 'INVH', 'ALGN', 'OMC', 'DGX', 'J', 'GPC', 'ARE', 'MAS', 'TPR', 'PNR', 'MOH', 'MAA', 'HOLX', 'TSN', 'CF', 'BAX', 'DG', 'LVS', 'EXPD', 'BALL', 'AVY', 'IEX', 'AFG', 'HLI', 'TXT', 'KIM', 'RVTY', 'DPZ', 'APTV', 'DLTR', 'DOC', 'JBHT', 'SWK', 'ROL', 'POOL', 'VTRS', 'AMCR', 'TLN', 'ALKT', 'VIAV', 'CLSK', 'CALX', 'IPGP', 'RAMP', 'PLUS', 'ACLS', 'EXTR', 'YOU', 'ERIE', 'UHS', 'SEIC', 'NWSA', 'MRNA', 'UGI', 'MORN', 'ORI', 'BEN', 'VEEV', 'IDA', 'AGNC', 'VECO', 'VRNT', 'VSH', 'KN', 'HUT', 'VYX', 'ROG', 'UCTT', 'PD', 'CTS', 'MXL', 'NTCT', 'ADEA', 'BHE', 'INFA', 'AVPT', 'AXS', 'RYAN', 'VOYA', 'WTI', 'REG', 'UDR', 'CHRW', 'ALLE', 'CAG', 'HST', 'TECH', 'NCLH', 'NDSN', 'KMX', 'BXP', 'EMN', 'SJM', 'CPT', 'FOXA', 'IPG', 'ALB', 'INCY', 'BG', 'TAP', 'HSIC', 'SOLV', 'GNRC', 'LKQ', 'DAY', 'RL', 'WYNN', 'CRL', 'AOS', 'WBA', 'HII', 'LW', 'TFX', 'CE', 'FRT', 'HRL', 'FAF', 'STWD', 'OMF', 'GRNT', 'NJR', 'RLI', 'COLB', 'SLM', 'AFRM', 'POR', 'MTG', 'MOS', 'MTCH', 'TPG', 'ZUO', 'SCSC', 'COHU', 'GDYN', 'NSSC', 'ARLO', 'WULF', 'INFN', 'ICHR', 'HLIT', 'PLAB', 'COMM', 'MTTR', 'DGII', 'JHG', 'TXNM', 'RITM', 'OTEX', 'BKH', 'SWX', 'PCTY', 'LAZ', 'OGS', 'SR', 'AMG', 'THG', 'ALE', 'ORA', 'MGEE', 'MDU', 'AGO', 'WTM', 'LNC', 'NWE', 'LNG', 'KMPR', 'VRT', 'AVA', 'AWR', 'VSAT', 'WOLF', 'APLD', 'INDI', 'AOSL', 'AMPL', 'YEXT', 'NN', 'SEDG', 'BLND', 'BELFB', 'HCKT', 'CNXN', 'CIFR', 'CEVA', 'SWI', 'PRO', 'PENG', 'ACMR', 'JAMF', 'OLO', 'NABL', 'XRX', 'CXM', 'PDFS', 'NTGR', 'OTTR', 'REI', 'CWT', 'CPK', 'NWS', 'MGM', 'CPB', 'CZR', 'PARA', 'BF-B', 'FMC', 'FHB', 'RY', 'TFSL', 'XP', 'CACC', 'VIRT', 'MHK', 'HAS', 'BWA', 'DVA', 'FOX', 'CWEN', 'WEX', 'FERG', 'MISXX', 'ALNY', 'RBLX', 'FOUR', 'CRBG', 'ONB', 'BHF', 'NEP', 'HE', 'NWN', 'SJW', 'COOP', 'JXN', 'CADE', 'WSM', 'CPNG', 'LWLG', 'VPG', 'CLFD', 'AEHR', 'SEMR', 'FARO', 'WEAV', 'ADTN', 'MLNK', 'RBBN', 'LASR', 'CRNC', 'CRSR', 'NVTS', 'EVLV', 'ETWO', 'MITK', 'EGHT', 'BIGC', 'ENFN', 'CCSI', 'MEI', 'OUST', 'OSPN', 'LGTY', 'DOMO', 'UIS', 'SMRT', 'MVIS', 'KE', 'DMRC', 'XPER', 'DBD', 'BASE', 'CVNA', 'SIGI', 'ESNT', 'PIPR', 'HLNE', 'MC', 'EME', 'TD', 'ILMN', 'BN', 'ENB', 'AMPY', 'RBA', 'RDN', 'SFBS', 'UPST', 'HWC', 'FBP', 'EEFT', 'PINS', 'NTRA', 'BURL', 'FIX', 'FWONK', 'TRU', 'DKNG', 'CSL', 'WSO', 'SSNC', 'TCBI', 'CNO', 'STEP', 'PJT', 'WU', 'ABCB', 'FCFS', 'ESGR', 'ASB', 'IBOC', 'AX', 'UCB', 'CNQ', 'CRH', 'CP', 'BNS', 'BMO', 'RS', 'OC', 'HEI-A', 'RPM', 'USFD', 'UTHR', 'SUI', 'XPO', 'UTL', 'CWEN-A', 'MSEX', 'RKT', 'AVTR', 'NBIX', 'ACM', 'CASY', 'CW', 'GGG', 'CM', 'ALAB', 'DKS', 'PFGC', 'FTI', 'TOL', 'SAIA', 'ITCI', 'CVBF', 'BGC', 'BHF', 'BKU', 'NMIH', 'SFNC', 'WSFS', 'CATY', 'WD', 'FHI', 'VCTR', 'RELY', 'FIBK', 'PRK', 'PLMR', 'BFH', 'HASI', 'GNW', 'APAM', 'BOH', 'BXMT', 'EBC', 'SNEX', 'PAYO', 'CBU', 'FLG', 'PFSI', 'FULT', 'INDB', 'AUB', 'MFC', 'QGEN', 'CLH', 'RPRX', 'AMH', 'BJ', 'NVT', 'BMRN', 'Z', 'THC', 'GLPI', 'WPC', 'ITT', 'ELS', 'DUOL', 'JLL', 'AAL', 'TXRH', 'RACE', 'TRP', 'SU', 'WCN', 'OKLO', 'BWXT', 'SRPT', 'CNH', 'ATR', 'RIVN', 'WWD', 'DTM', 'RRX', 'GME', 'TKO', 'BLD', 'TTEK', 'LECO', 'LAMR', 'HEI', 'SCI', 'CNM', 'FND', 'ALC', 'NBTB', 'FFBC', 'FLYW', 'BWIN', 'HTLF', 'NATL', 'OSCR', 'BANR', 'BANF', 'PFS', 'SBCF', 'ABR', 'RNST', 'EFSC', 'TFIN', 'TBBK', 'GSHD', 'ENVA', 'OFG', 'BANC', 'CNS', 'TOWN', 'FRME', 'WAFD', 'LMND', 'TRMK', 'EVTC', 'PPBI', 'SYBT', 'CNA', 'CACI', 'EHC', 'AYI', 'PEN', 'DOX', 'SKX', 'CCK', 'EXEL', 'EXAS', 'AA', 'OHI', 'LAD', 'BRBR', 'GMED', 'TPX', 'MTZ', 'CUBE', 'ROKU', 'ARMK', 'RGEN', 'ALSN', 'RGLD', 'AEM', 'YORW', 'FYBR', 'EGP', 'BBWI', 'KBR', 'ALK', 'AAON', 'INGR', 'G', 'FLS', 'LNW', 'LBRDK', 'AXTA', 'TTC', 'RBC', 'WCC', 'VFC', 'REXR', 'KNX', 'CHE', 'CHDN', 'ACI', 'GLOB', 'WMS', 'WING', 'WH', 'APG', 'X', 'CAVA', 'FBIN', 'EXP', 'SLF', 'WHR', 'WSC', 'CART', 'SN', 'SSD', 'MASI', 'RHI', 'R', 'BRX', 'FRPT', 'DOCS', 'LPX', 'BFAM', 'TREX', 'FR', 'ATI', 'VMI', 'VNO', 'FCN', 'JAZZ', 'ADC', 'ESAB', 'AZEK', 'NOVA', 'BHLB', 'BUSE', 'SKWD', 'SPNT', 'STC', 'WSBC', 'TRUP', 'PWP', 'CUBI', 'MQ', 'DCOM', 'LOB', 'TWO', 'LKFN', 'PRG', 'WABC', 'HOPE', 'HMN', 'AVDX', 'DBRG', 'EIG', 'DFIN', 'LC', 'QCRH', 'GABC', 'SAFT', 'CODI', 'CHCO', 'FBK', 'NNI', 'FCF', 'NIC', 'NWBI', 'NBHC', 'LADR', 'VRTS', 'VBTX', 'HTH', 'SASR', 'STEL', 'MCY', 'STBA', 'CASH', 'AGM', 'TCBK', 'RKT', 'WT', 'WPM', 'BSY', 'BRKR', 'BIO', 'COLD', 'SAIC', 'SITE', 'UHAL-B', 'AWI', 'AM', 'ETSY', 'MTN', 'ESI', 'AGCO', 'STAG', 'DAR', 'CROX', 'BAM', 'FNV', 'NTR', 'FTS', 'TRI', 'UWMC', 'CVE', 'QSR', 'MNTK', 'ARTNA', 'GNE', 'AMPS', 'TEF', 'KRNY', 'NRDS', 'EZPW', 'ARI', 'HTBK', 'ROOT', 'CPF', 'TMP', 'BHRB', 'CMTG', 'BOW', 'CCNE', 'CFB', 'CIM', 'PFBC', 'IBCP', 'RC', 'MFA', 'MBIN', 'RWT', 'CNNE', 'HAFC', 'ML', 'THFF', 'TREE', 'CARE', 'BRKL', 'BRSP', 'HFWA', 'SEZL', 'HBNC', 'CFFN', 'CASS', 'SRCE', 'BY', 'CCB', 'CCBG', 'CAC', 'CNOB', 'RPAY', 'TIPT', 'IVR', 'SMBK', 'SPFI', 'AAMI', 'SMBC', 'SBSI', 'HG', 'GLRE', 'GSBC', 'HIPO', 'MBWM', 'MCB', 'DX', 'PAY', 'PFC', 'GDOT', 'ECPG', 'NBBK', 'AMBC', 'NAVI', 'WASH', 'HTBI', 'OCFC', 'FMNB', 'DHIL', 'PEBO', 'CTLP', 'MSBI', 'UFCS', 'ORC', 'OSBC', 'HONE', 'LPRO', 'PX', 'BFST', 'PRA', 'TRST', 'TRTX', 'BFC', 'PRAA', 'BHB', 'PGC', 'PMT', 'HCI', 'CTBI', 'UVSP', 'EGBN', 'UVE', 'FBRT', 'FCBC', 'FBMS', 'NFBK', 'FFWM', 'AMTB', 'AMSF', 'KREF', 'OBK', 'FMBH', 'EQBK', 'EFC', 'AROW', 'UWMC', 'ARR', 'NYMT', 'IMXI', 'IIIV', 'AMAL', 'GFL', 'TFII', 'IMO', 'STN', 'CLS', 'BCE', 'PAAS', 'GIL', 'CAE', 'CIGI', 'WFG', 'AGI', 'FSV'];

# ---------------------------------------------------------------------------
# Fetch and store raw data
# ---------------------------------------------------------------------------

# DONT run everytime
# =============================================================================
fetcher = DataFetcher(db_name=AV_db_file, api_key=AV_api_key)

# Fundamental reports: overview, income statement, balance sheet, cash flow
fetcher.store_all_fundamentals(tickers_R3000, period='quarterly')

# Daily price history
fetcher.store_daily_prices(tickers_R3000, outputsize="full")

# News sentiment
fetcher.store_news_sentiment(tickers_R3000)

# Technical indicators we want to store
TECHNICALS = [
    ("SMA", 20),
    ("EMA", 20),
    ("RSI", 14),
]

for ticker in tickers_R3000:
    for indicator, period in TECHNICALS:
        fetcher.store_technical_indicator(ticker, indicator, time_period=period)

print("Raw data downloaded and stored.")


# ---------------------------------------------------------------------------
# Access stored data (runs in 3 mins)
# ---------------------------------------------------------------------------
accessor = DatabaseAccessor(db_name=AV_db_file)
prices = accessor.get_prices(tickers_R3000)
fundamentals = accessor.get_fundamentals(tickers_R3000)

# Example join of prices with company overview
#price_overview = accessor.get_prices_with_overview(tickers_R3000)

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
engineer = FeatureEngineer(accessor)
ratios = engineer.compute_financial_ratios(tickers_R3000) ##code ran till this line ?whats diff btw price & price_overview
volatility = engineer.rolling_volatility(prices, window=20)

# Merge engineered features with daily prices
full_data = prices.merge(ratios, on="ticker", how="left")
full_data = full_data.merge(volatility[["ticker", "date", "volatility"]],
                            on=["ticker", "date"], how="left")

# ``full_data`` now contains price history along with basic ratios and
# volatility estimates. This DataFrame can be used for modeling or
# regression analysis.
print(full_data.tail())

full_data.ticker.unique_values()

# Export data to excel spreadsheet:
# Create an ExcelWriter object
writer = pd.ExcelWriter('data_june12.xlsx', engine='xlsxwriter')

fundamentals['balance_sheet'].to_excel(writer, "balance_sheet.xlsx")
fundamentals['overview'].to_excel(writer, "overview.xlsx")
fundamentals['income_statement'].to_excel(writer, "income_statement.xlsx")
fundamentals['cash_flow'].to_excel(writer, "cash_flow.xlsx")
# save excel
writer.close()


print(price_overview.head())


