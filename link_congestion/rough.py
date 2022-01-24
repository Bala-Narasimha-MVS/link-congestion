
import mysql.connector as mycon
from datetime import datetime, time, timedelta
import pandas as pd
global dbtype
dbtype = "mysql"


def get_sat_flow(linkSCN, dbcreds, reality, linkerfile):
    ''' Get saturation flow from utmc_transport_link_data_dynamic.POC ONLY.'''

    return 0


def insert_into_kpi_report_table(linkTTDict, linkAttribDict, linker, dbcreds):
    '''
        Function to insert kpi values into kpi report table
    '''
    data_dict = {}
    df = pd.read_csv(linker)
    for alink in linkAttribDict:
        junc = linkAttribDict[alink]["junctionSCN"]
        group = df.loc[df["junctionSCN"] == junc]["GroupSCN"].iloc[0]
        if junc not in data_dict:
            data_dict[junc] = {"KPI": linkTTDict[alink]
                               ["kpi"], "Group_SCN": group}

    ist_time = datetime.utcnow().replace(microsecond=0) + \
        timedelta(minutes=30, hours=5)
    global dbtype
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3])
        htms_cursor = htmsdb.cursor()

        htms_cursor.execute(
            "SELECT SignalSCN, ShortDescription FROM utmc_traffic_signal_static;")
        raw_data = htms_cursor.fetchall()
        map_dict = {}
        for doublet in raw_data:
            map_dict[doublet[0]] = doublet[1]

        for junc in data_dict:

            sql = "INSERT INTO atcs_kpi_report (Group_SCN,SignalName,KPI,TimeStamp,LastUpdated ) VALUES ( %s,%s,%s,%s,%s)"
            htms_cursor.execute(
                sql, (data_dict[junc]["Group_SCN"], map_dict[junc], data_dict[junc]["KPI"], ist_time, ist_time))
        htmsdb.commit()

    elif dbtype == "postgresql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        htms_cursor.execute(
            """SELECT "SignalSCN", "ShortDescription" FROM utmc_traffic_signal_static;""")
        raw_data = htms_cursor.fetchall()
        map_dict = {}
        for doublet in raw_data:
            map_dict[doublet[0]] = doublet[1]

        for junc in data_dict:

            sql = """INSERT INTO atcs_kpi_report ("Group_SCN","SignalName","KPI","TimeStamp","LastUpdated" ) VALUES ( %s,%s,%s,%s,%s)"""
            htms_cursor.execute(
                sql, (data_dict[junc]["Group_SCN"], map_dict[junc], data_dict[junc]["KPI"], ist_time, ist_time))
        htmsdb.commit()

    htms_cursor.close()
    htmsdb.close()


if __name__ == "__main__":
    linkTTdict = {"LINK005": {"junctionSCN": "J003", "kpi": 500}, "LINK006": {
        "junctionSCN": "J003", "kpi": 0.755}, "LINK817": {"junctionSCN": "J051", "kpi": 50}}
    linkAttribDict = {"LINK005": {"junctionSCN": "J003", "kpi": 50}, "LINK006": {
        "junctionSCN": "J003", "kpi": 0.7}, "LINK817": {"junctionSCN": "J051", "kpi": 50}}
    linker = "D:/algorithms/Helpers/Linker.csv"
    dbcreds = ["localhost", "root", "password", "htms", 5444]
    insert_into_kpi_report_table(linkTTdict, linkAttribDict, linker, dbcreds)
