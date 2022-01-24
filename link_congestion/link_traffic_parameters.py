# ***************************************************
# Author : Venu Myneni
# Developed at : ITS Planners & Engineers Pvt Ltd.
# Date : 26-08-2020
# ***************************************************


## General Flow ###########################################################
# 1) get all the linKSCNs and their attributes from a csv or a db or both
# 2) get the time period during which the veh counts are to be used to update link tt
# 3) calculate the link tt based on those counts and current plan running
# 4) update link tt to the database (in addition flow, avg spd, congestion too)
# 5) write a cronjob at the required frequency
###########################################################################
# from asyncio.windows_events import NULL
import mysql.connector as mycon
import psycopg2 as mycop


import pandas as pd

from datetime import datetime, timedelta
import random
import copy


#from itspelogger import Logger
# logger=Logger("LTP")

def get_current_plan_dict(junctionId, dbcreds):
    ''' A dictionary of key:linkOrder, val:[execOrder,stageTime] for current plan at ajunction.'''
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])

        htms_cursor = htmsdb.cursor()
        htms_cursor.execute(
            "SELECT currentPlan FROM utmc_traffic_signal_static WHERE SignalSCN='"+str(junctionId)+"';")
        raw_data = htms_cursor.fetchall()

        planId = raw_data[0][0]
        htms_cursor.execute(
            "SELECT PlanSCN FROM plans WHERE ID='"+str(planId)+"';")
        raw_data = htms_cursor.fetchall()

        planscn = raw_data[0][0]
        atuple = (planscn,)
        htms_cursor.execute(
            "SELECT execOrder,StageNumber,StageTime FROM signal_timings WHERE Plan_SCN = %s and execOrder >0 ORDER BY execOrder ASC;", atuple)
        raw_data = htms_cursor.fetchall()
        out_dict = {}
        linkOrder = 1
        for i in range(0, len(raw_data)):
            # key = stagenum, val =exOrd,stgTm
            out_dict[linkOrder] = [raw_data[i][0], raw_data[i][2]]
            linkOrder += 1

    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])

        htms_cursor = htmsdb.cursor()
        htms_cursor.execute(
            'SELECT "currentPlan" FROM utmc_traffic_signal_static WHERE "SignalSCN"=%s;', (junctionId,))
        raw_data = htms_cursor.fetchall()
        #print(raw_data)
        planId = raw_data[0][0]
        htms_cursor.execute(
            'SELECT "PlanSCN" FROM plans WHERE "ID"=%s;', (planId,))
        raw_data = htms_cursor.fetchall()
        #print(raw_data)

        planscn = raw_data[0][0]
        atuple = (planscn,)
        htms_cursor.execute(
            'SELECT "execOrder","StageNumber","StageTime" FROM signal_timings WHERE "Plan_SCN" = %s and "execOrder" >0 ORDER BY "execOrder" ASC;', atuple)
        raw_data = htms_cursor.fetchall()
        out_dict = {}
        linkOrder = 1
        for i in range(0, len(raw_data)):
            out_dict[linkOrder] = [raw_data[i][0], raw_data[i][2]]
            linkOrder += 1

    htms_cursor.close()
    htmsdb.close()
    ##print("Out Dict : {}".format(out_dict))
    return out_dict


def get_veh_counts_pred_multi_dets(dets_list, nMin, dbCreds):
    """
        Gives out a dictionary of key:detId, val:predicted counts in the next N minutes.

    """

    dbtype = dbCreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3], port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
        params_dict = {"end_time": datetime.now()+timedelta(minutes=nMin),
                       "start_time": datetime.now(), "dets_list": tuple(dets_list)}

        query = 'SELECT SystemCodeNumber,CurrentFlow FROM utmc_transport_link_prediction_data WHERE SystemCodeNumber IN {} AND StartTime >= %s AND EndTime <= %s;'.format(
            params_dict["dets_list"])
    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3], port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
        params_dict = {"end_time": datetime.now()+timedelta(minutes=nMin),
                       "start_time": datetime.now(), "dets_list": tuple(dets_list)}

        query = 'SELECT "SystemCodeNumber","CurrentFlow" FROM utmc_transport_link_prediction_data WHERE "SystemCodeNumber" IN {} AND "StartTime" >= %s AND "EndTime" <= %s;'.format(
            params_dict["dets_list"])

    htms_cursor.execute(
        query, (params_dict["start_time"], params_dict["end_time"],))

    raw_data = htms_cursor.fetchall()

    htms_cursor.close()
    htmsdb.close()

    out_dict = {}
    for twolet in raw_data:
        det_id = twolet[0]
        total = twolet[1]
        if det_id in out_dict:
            out_dict[det_id] = out_dict[det_id] + total
        else:
            out_dict[det_id] = total

    for det in dets_list:
        if det not in out_dict:
            out_dict[det] = 0

    return out_dict


def get_veh_counts_last_multi_dets(dets_list, nMin, dbCreds):
    """
        Gives out a dictionary of key:detId, val:predicted counts in the last N minutes.

    """

    dbtype = dbCreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3], port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
        params_dict = {"start_time": datetime.now()-timedelta(minutes=nMin),
                       "end_time": datetime.now(), "dets_list": tuple(dets_list)}

        query = 'SELECT SystemCodeNumber,TotalFlow FROM utmc_detector_dynamic WHERE SystemCodeNumber IN {} AND LastUpdated >= %s AND LastUpdated <= %s;'.format(
            params_dict["dets_list"])
        htms_cursor.execute(
            query, (params_dict["start_time"], params_dict["end_time"]))
    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3], port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
        params_dict = {"start_time": datetime.now()-timedelta(minutes=nMin),
                       "end_time": datetime.now(), "dets_list": tuple(dets_list)}

        query = 'SELECT "SystemCodeNumber","TotalFlow" FROM utmc_detector_dynamic WHERE "SystemCodeNumber" IN {} AND "LastUpdated" >= %s AND "LastUpdated" <= %s;'.format(
            params_dict["dets_list"])
        htms_cursor.execute(
            query, (params_dict["start_time"], params_dict["end_time"]))

    raw_data = htms_cursor.fetchall()

    htms_cursor.close()
    htmsdb.close()

    out_dict = {}
    for twolet in raw_data:
        det_id = twolet[0]
        total = twolet[1]
        if det_id in out_dict:
            out_dict[det_id] = out_dict[det_id] + total
        else:
            out_dict[det_id] = total

    for det in dets_list:
        if det not in out_dict:
            out_dict[det] = 0
    # print('out_dict', out_dict)
    return out_dict


def get_sat_flow(linkSCN, dbcreds, reality, linkerfile):
    ''' Get saturation flow from utmc_transport_link_data_dynamic'''
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()
        atuple = (linkSCN,)
        query = "SELECT SaturationFlow FROM utmc_saturation_flow WHERE SystemCodeNumber =%s  AND HistoricDate IS NULL;"
        htms_cursor.execute(query, atuple)
    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()
        atuple = (linkSCN,)
        query = 'SELECT "SaturationFlow" FROM utmc_saturation_flow WHERE "SystemCodeNumber" =%s AND "HistoricDate" IS NULL;'
        htms_cursor.execute(query, atuple)

    raw_data = htms_cursor.fetchall()
    if len(raw_data) > 0:
        total_sat_flow = 0
        for singlet in raw_data:
            total_sat_flow += singlet[0]
        sat_flow = round(total_sat_flow/len(raw_data), 0)
    else:
        # read default from linker if exists
        df = pd.read_csv(linkerfile)
        mask = (df["linkSCN"] == linkSCN)
        df = df.loc[mask]
        if df.shape[0] > 0:
            sat_flow = df["defaultSatFlow"].iloc[0]
            if sat_flow == "x":
                return 0
        else:
            sat_flow = 0  # if no data

    return sat_flow


def get_link_attribs(linker, def_sat, dbcreds, default_dict, junctions="ALL"):
    ''' Get physical attributes of a link.
        Return(dict):
        {
        LINK001:{NumLanes:2,linkOrd:3,junctionSCN:J001,detRealids:[],satFlow:3456,greenShare:0.22,numPhases:4,length:100,designSpeed:8},
        LINK002:{},
        }
    '''
    linkAttribDict = {}
    df = pd.read_csv(linker)
    df = df.set_index("linkSCN")
    for link in df.index:
        junc = df.loc[link, "junctionSCN"]
        if junctions == "ALL" or junc in junctions:
            linkAttribDict[link] = {}
            num_lanes = df.loc[link, "NumLanes"]
            if num_lanes == "x":
                num_lanes = 2
            else:
                num_lanes = int(num_lanes)
            linkAttribDict[link]["NumLanes"] = int(num_lanes)
            linkAttribDict[link]["linkOrd"] = int(df.loc[link, "linkOrder"])
            linkAttribDict[link]["junctionSCN"] = df.loc[link, "junctionSCN"]
            linkAttribDict[link]["detRealIds"] = df.loc[link,
                                                        "detRealIds"].split(",")

            try:
                satFlow = get_sat_flow(link, dbcreds, "SAT", linker)
            except:
                satFlow = 0
                #print("Warning :utmc_saturation_flow probably doesn't exist.")
            if satFlow == 0:
                linkAttribDict[link]["satFlow"] = def_sat * \
                    num_lanes
            else:
                linkAttribDict[link]["satFlow"] = satFlow

            # length, design speed, share of green
            linkAttribDict[link]["greenShare"] = round(
                1/df.loc[link, "numPhases"], 2)
            linkAttribDict[link]["length"] = default_dict["linkLength"]  # m
            # m/s [MoRTH @ Urban Roads @ 70 kmph] we asuming 54 kmph
            linkAttribDict[link]["designSpeed"] = default_dict["designSpeed"]

    return linkAttribDict


def get_congestion_based_tt(congestion_ratio, min_tt, max_tt):
    if congestion_ratio <= 0.10:
        return min_tt
    elif congestion_ratio < 1:
        return int(min(min_tt + congestion_ratio*min_tt, max_tt))
    else:
        return int(min(min_tt + (congestion_ratio**2)*min_tt, max_tt))


def get_congestion(cur_flow, sat_flow, def_sat, green_share, cong_factor):
    '''
        output : ex: 0.1, 0.9
        Description : Simply as ratio of cur flo and sat flo multiplied by a congestion_factor (cf).

    '''
    if sat_flow == 0:
        sat_flow = def_sat*2
    ##print("CUR_FLOW: {} , SAT_FLOW: {}, GS: {}".format(cur_flow, sat_flow, green_share))
    return round(cur_flow*cong_factor/(sat_flow*green_share), 2)


def get_link_tt(linkAttribDict, nMin, def_sat, dbCreds):
    """
    Output: a dictionary
    {"LINK005":{"tt":50, "curflo":300, "avgspd":8}, "LINK006":{"tt":62, "curflo":400,"avgspd":7}}

    """
    linkTTdict = {}

    junction_cyctime_dict = {}

    # all dets at once
    dets_list = []
    for link in linkAttribDict:
        for det in linkAttribDict[link]["detRealIds"]:
            dets_list.append(det)
    
    dets_last_n_count = get_veh_counts_last_multi_dets(
        dets_list, nMin, dbCreds)
    print('dets_last_n_count', dets_last_n_count)
    
    for link in linkAttribDict:
        ##print("LINK : {}".format(link))
        linkTTdict[link] = {}
        # getting num of vehs
        totalVehs = 1  # setting to 1 instead of 0 to avoid zero divison error
        for det in linkAttribDict[link]["detRealIds"]:
            totalVehs += dets_last_n_count[det]
        
        junctionId = linkAttribDict[link]["junctionSCN"]

        # getting cycle time
        cur_plan_dict = {}
        if junctionId in junction_cyctime_dict:
            cycTm = junction_cyctime_dict[junctionId]
        else:
            # try:
            cur_plan_dict = get_current_plan_dict(junctionId, dbCreds)
            cycTm = 0
            for akey in cur_plan_dict:
                cycTm += cur_plan_dict[akey][1]

            '''
            except:
                cycTm = 90
                #print("Problem with get_currentPlan_cycleTime for junction : {}".format(junctionId))
            '''
            # cycTm = max(cycTm, 76)
            junction_cyctime_dict[junctionId] = cycTm

        # getting avg travel time
        base_tt = round(
            linkAttribDict[link]["length"] / linkAttribDict[link]["designSpeed"], 2)
        if len(cur_plan_dict) > 0:
            linkOrder = linkAttribDict[link]["linkOrd"]
            if linkOrder == "x":
                green_share = linkAttribDict[link]["greenShare"]
            elif linkOrder in cur_plan_dict:
                green_share = round(cur_plan_dict[linkOrder][1]/cycTm, 2)
            else:
                green_share = 0.20
        else:
            green_share = linkAttribDict[link]["greenShare"]
        print('link', link)
        totalCycles = round(nMin * 60 / cycTm, 2)
        totalGreenDur = round(green_share * totalCycles *
                              cycTm, 2)  # 1 in 4 stages

        if linkAttribDict[link]["NumLanes"] == "x":
            headway = 2.5
        else:
            if linkAttribDict[link]["NumLanes"] != 0:
                headway = round(
                    (linkAttribDict[link]["satFlow"]/linkAttribDict[link]["NumLanes"])/3600, 2)
            else:
                headway = 2.5

        veh_pass_limit = (totalGreenDur/headway) * \
            linkAttribDict[link]["NumLanes"]  # 2= time headway

        if veh_pass_limit >= totalVehs:
            vehs_during_green = green_share * totalVehs
            vehs_during_red = (1-green_share) * totalVehs
            tot_tt = vehs_during_green * base_tt + vehs_during_red * \
                (base_tt + (1-green_share) * 1/2 * cycTm)
            avg_tt = round(tot_tt/totalVehs, 2)

        elif veh_pass_limit < totalVehs:
            remain_vehs = totalVehs - veh_pass_limit
            vehs_during_green = green_share * veh_pass_limit
            vehs_during_red = (1-green_share) * veh_pass_limit
            tot_tt = vehs_during_green * base_tt + vehs_during_red * \
                (base_tt + (1-green_share) * 1/2 * cycTm) + \
                remain_vehs * cycTm  # assuming one cycle failure
            avg_tt = round(tot_tt/totalVehs, 2)

        if totalVehs > 1:
            linkTTdict[link]['DOS'] = totalVehs*60/(linkAttribDict[link]["satFlow"]*nMin)
        else:
            linkTTdict[link]['DOS'] = 0
        # adding to out dict
        ##print("nvehs : ", totalVehs)
    
        linktt_min = round(
            linkAttribDict[link]["length"]/linkAttribDict[link]["designSpeed"], 0)

        if totalVehs != 1:
            linkTTdict[link]["curflo"] = round(totalVehs * 60 / nMin, 0)  # vph
        else:
            linkTTdict[link]["curflo"] = 0

        # congestion
        sat_flow = linkAttribDict[link]["satFlow"]

        linkTTdict[link]["congestion"] = get_congestion(
            linkTTdict[link]["curflo"], sat_flow, def_sat, green_share, 1)

        linkTTdict[link]["tt"] = get_congestion_based_tt(
            linkTTdict[link]["congestion"], linktt_min, 1.5*cycTm)

        linkTTdict[link]["avgspd"] = round(
            linkAttribDict[link]["length"] / avg_tt, 2)  # m/sec

    return linkTTdict


def add_kpi(linkAttribDict, linkTTdict):
    # get same junction links
    same_junc_dict = {}
    for link in linkAttribDict:
        junctionId = linkAttribDict[link]["junctionSCN"]

        if junctionId in same_junc_dict:
            same_junc_dict[junctionId].append(link)
        else:
            same_junc_dict[junctionId] = [link]

    for link in linkTTdict:
        junctionId = linkAttribDict[link]["junctionSCN"]
        links_list = same_junc_dict[junctionId]
        kpi_list = [linkTTdict[i]["congestion"] for i in links_list]
        flow_list = [linkTTdict[i]["curflo"] for i in links_list]
        linkTTdict[link]["kpi"] = round(
            sum([a*b for a, b in zip(kpi_list, flow_list)])/max(sum(flow_list), 1), 2)
    return linkTTdict


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
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        htms_cursor.execute(
            "SELECT SignalSCN, ShortDescription FROM utmc_traffic_signal_static;")
        raw_data = htms_cursor.fetchall()
        map_dict = {}
        for doublet in raw_data:
            map_dict[doublet[0]] = doublet[1]

        for junc in data_dict:

            sql = "INSERT INTO atcs_kpi_report (Group_SCN,SignalName,KPI,TimeStamp,LastUpdated ) VALUES ( %s,%s,%s,%s,%s)"
            htms_cursor.execute(sql, (data_dict[junc]["Group_SCN"], map_dict[junc], float(
                data_dict[junc]["KPI"]), ist_time, ist_time))
        htmsdb.commit()

    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
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


def get_link_tt_extension(linkTTdict, linkAttribDict, extDict):
    ''' Get traffic state parameters for extension links.'''
    linkTTdictExt = {}
    for link in linkTTdict:
        extLink = link+"_ext"
        if extLink in extDict:
            linkTTdictExt[extLink] = {}
            tt = round(extDict[extLink]/15, 0)
            curflo = linkTTdict[link]["curflo"]
            avgspd = linkTTdict[link]["avgspd"]
            linkTTdictExt[extLink]["tt"] = round(tt, 0)
            linkTTdictExt[extLink]["curflo"] = round(curflo, 0)
            linkTTdictExt[extLink]["avgspd"] = round(avgspd, 2)
            linkTTdictExt[extLink]["congestion"] = linkTTdict[link]["congestion"]
    for link in extDict:
        if link not in linkTTdictExt:
            linkTTdictExt[link] = {}
            tt = extDict[link]/10  # 10m/s
            tt = random.uniform(tt*0.85, 1.15*tt)
            curflo = random.uniform(20, 100)
            avgspd = random.uniform(6, 8)
            linkTTdictExt[link]["tt"] = round(tt, 0)
            linkTTdictExt[link]["curflo"] = round(curflo, 0)
            linkTTdictExt[link]["avgspd"] = round(avgspd, 2)
            linkTTdictExt[link]["congestion"] = 0.5
    return linkTTdictExt


def update(dbcreds, reality):
    ''' Updates data in a table for normal links'''
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        sql = """UPDATE utmc_transport_link_data_dynamic SET HistoricDate = current_timestamp() WHERE Reality = %s AND HistoricDate is NULL;"""
        htms_cursor.execute(sql, (reality,))

    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        sql = 'UPDATE utmc_transport_link_data_dynamic SET "HistoricDate" = %s WHERE "Reality" = %s AND "HistoricDate" is NULL;'
        htms_cursor.execute(
            sql, (datetime.now(), reality,))

    htmsdb.commit()

    htms_cursor.close()
    htmsdb.close()


def update_ext(dbcreds, reality, sub_str):
    ''' Updates data in a table for extension links.'''
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        sql = """UPDATE utmc_transport_link_data_dynamic SET HistoricDate = current_timestamp() WHERE Reality = %s AND HistoricDate is NULL AND SystemCodeNumber LIKE %s;"""
        htms_cursor.execute(sql, (reality, "%"+sub_str,))

    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        sql = 'UPDATE utmc_transport_link_data_dynamic SET "HistoricDate" = %s WHERE "Reality" = %s AND "HistoricDate" is NULL AND "SystemCodeNumber" LIKE %s;'
        htms_cursor.execute(
            sql, (datetime.now(), reality, "%"+sub_str,))

    htmsdb.commit()

    htms_cursor.close()
    htmsdb.close()





def insert(linkTTdict, table, dbcreds, reality):
    ''' Inserts data from a dictionary into a table for a given reality.'''
    dbtype = dbcreds[-1]
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        for link in linkTTdict:
            myDict = {}
            myDict["SystemCodeNumber"] = link
            myDict["CurrentFlow"] = float(linkTTdict[link]["curflo"])
            myDict["AverageSpeed"] = float(linkTTdict[link]["avgspd"])
            myDict["LinkTravelTime"] = float(linkTTdict[link]["tt"])
            myDict["CongestionPercent"] = float(
                linkTTdict[link]["congestion"])  # as demand/capacity ratio
            # should be picked from TIM Lua script
            myDict["OccupancyPercent"] = float(
                min(round(linkTTdict[link]["congestion"]*random.uniform(0.75, 0.85), 2), 1))
            myDict["Reality"] = reality
            myDict['DegreeOfSaturation'] = (linkTTdict[link]['DOS'])
            try:
                myDict["Colour"] = float(linkTTdict[link]["kpi"])
            except:
                myDict["Colour"] = 0.5

            placeholders = ', '.join(['%s'] * len(myDict))
            columns = ', '.join(myDict.keys())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (
                table, columns, placeholders)
            # valid in Python 2
            #htms_cursor.execute(sql, myDict.values())
            htms_cursor.execute(sql, list(myDict.values()))

            htmsdb.commit()
    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        for link in linkTTdict:
            myDict = {}
            myDict["SystemCodeNumber"] = link
            myDict["CurrentFlow"] = float(linkTTdict[link]["curflo"])
            myDict["AverageSpeed"] = float(linkTTdict[link]["avgspd"])
            myDict["LinkTravelTime"] = float(linkTTdict[link]["tt"])
            myDict["CongestionPercent"] = float(linkTTdict[link]["congestion"])
            myDict["OccupancyPercent"] = float(
                min(round(linkTTdict[link]["congestion"]*random.uniform(0.75, 0.85), 2), 1))
            myDict["Reality"] = reality
            myDict['DegreeOfSaturation'] = float(linkTTdict[link]['DOS'])
            try:
                myDict["KPI"] = float(linkTTdict[link]["kpi"])
            except:
                myDict["KPI"] = 0.5

            sql = 'INSERT INTO utmc_transport_link_data_dynamic ( "SystemCodeNumber","CurrentFlow","AverageSpeed","LinkTravelTime","Reality","CongestionPercent","Colour","OccupancyPercent", "DegreeOfSaturation ) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s );'
            atuple = (myDict["SystemCodeNumber"], myDict["CurrentFlow"], myDict["AverageSpeed"], myDict["LinkTravelTime"],
                      myDict["Reality"], myDict["CongestionPercent"], myDict["KPI"], myDict["OccupancyPercent"],myDict['DegreeOfSaturation'])
            
            htms_cursor.execute(sql, atuple)

            htmsdb.commit()
    htms_cursor.close()
    htmsdb.close()


def get_predicted(linkTTdict, linkAttribDict, nMin, dbCreds):
    """
    Output: a dictionary
    {"LINK005":{"tt":50, "curflo":300, "avgspd":8}, "LINK006":{"tt":62, "curflo":400,"avgspd":7}}

    """

    link_next_n_count = get_veh_counts_pred_multi_dets(
        linkAttribDict.keys(), nMin, dbCreds)
    outDict = {}
    for link in linkTTdict:
        outDict[link] = {}
        ## patch #################################
        # getting num of vehs
        totalVehs = 1  # setting to 1 instead of 0 to avoid zero divison error
        if "_ext" in link:
            linky = link[:-4]
        else:
            linky = link
        if linky in linkAttribDict:
            totalVehs = link_next_n_count[linky]
        ##########################################
        if totalVehs >= 5:
            outDict[link]["curflo"] = round(totalVehs, 0)*60/nMin
        else:
            outDict[link]["curflo"] = linkTTdict[link]["curflo"]
        linktt = outDict[link]["curflo"] * \
            linkTTdict[link]["tt"] / max(1, linkTTdict[link]["curflo"])
        linktt_min = round(
            linkAttribDict[link]["length"]/linkAttribDict[link]["designSpeed"], 0)
        outDict[link]["tt"] = round(max(linktt, linktt_min, 0))
        outDict[link]["avgspd"] = round(linkTTdict[link]["avgspd"], 2)
        outDict[link]["congestion"] = round(
            outDict[link]["curflo"]*linkTTdict[link]["congestion"]/max(linkTTdict[link]["curflo"], 1), 2)
    return outDict


def get_estimated(linkTTdict):
    """
    Output: a dictionary
    {"LINK005":{"tt":50, "curflo":300, "avgspd":8}, "LINK006":{"tt":62, "curflo":400,"avgspd":7}}

    """
    outDict = {}
    for link in linkTTdict:
        outDict[link] = {}
        linkflo = linkTTdict[link]["curflo"]
        outDict[link]["curflo"] = round(random.uniform(
            int(0.9 * linkflo), int(1.1 * linkflo)), 0)
        linktt = outDict[link]["curflo"] * \
            linkTTdict[link]["tt"] / max(1, linkTTdict[link]["curflo"])
        outDict[link]["tt"] = round(linktt, 0)
        outDict[link]["avgspd"] = round(linkTTdict[link]["avgspd"], 2)
        outDict[link]["congestion"] = round(
            outDict[link]["curflo"]*linkTTdict[link]["congestion"]/max(linkTTdict[link]["curflo"], 1), 2)
    return outDict


def get_extension_links(dbcreds):
    ''' get extension links from the database'''
    dbtype = dbcreds[-1]
    outDict = {}
    if dbtype == "mysql":

        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        query = "SELECT SystemCodeNumber,LinkDistance from utmc_transport_link_static ;"
    elif dbtype == "postgresql":
        htmsdb = mycon.connect(
            host=dbcreds[0], user=dbcreds[1], password=dbcreds[2], database=dbcreds[3], port=dbcreds[4])
        htms_cursor = htmsdb.cursor()

        query = 'SELECT "SystemCodeNumber","LinkDistance" from utmc_transport_link_static ;'

    htms_cursor.execute(query)
    raw_data = htms_cursor.fetchall()
    htms_cursor.close()
    htmsdb.close()
    for doublet in raw_data:
        if "_ext" in doublet[0]:
            #print(doublet)
            outDict[doublet[0]] = doublet[1]
    return outDict


def no_det_links(linker, groups_list):
    ''' Returns list of links which have no detectors.'''
    no_det_link = []
    df = pd.read_csv(linker)
    df = df.set_index("linkSCN")
    for link in df.index:
        if ((df.loc[link, "sumoEdgeID"] == "x") and (df.loc[link, "GroupSCN"] in groups_list)):
            no_det_link.append(link)
    return no_det_link


def create_data_for_no_det_links(link_list, reality):
    '''
        GUI TESTING ONLY.
    '''
    myDict = {}
    for link in link_list:
        myDict[link] = {}
        myDict[link]["curflo"] = round(
            random.uniform(int(0.9 * 120), int(1.1 * 120)), 0)
        myDict[link]["avgspd"] = 5
        myDict[link]["tt"] = round(
            random.uniform(int(0.9 * 30), int(1.1 * 60)), 0)
        myDict[link]["congestion"] = round(random.uniform(0, 1), 2)

    return myDict


def get_link_attribs_ext_links(linkAttribDict, extDict, def_sat):
    ''' Get/Set link attributes of extension links.'''
    outDict = {}
    for link in linkAttribDict:
        ext_link = link + "_ext"
        if ext_link in extDict:
            outDict[ext_link] = copy.deepcopy(linkAttribDict[link])

    for link in extDict:
        if link not in outDict:
            outDict[link] = {}
            outDict[link]["NumLanes"] = 2
            outDict[link]["junctionSCN"] = "J"
            # #print(df.loc[link,"detRealIds"])
            outDict[link]["detRealIds"] = ["x", "x"]
            outDict[link]["satFlow"] = def_sat*2
            # length, design speed, share of green
            outDict[link]["greenShare"] = 0.20
            outDict[link]["length"] = 200  # m
            outDict[link]["designSpeed"] = 8
    return outDict


if __name__ == "__main__":
    pass
