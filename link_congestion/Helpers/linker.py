# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 10:59:22 2020

@author: venu
"""
import xml.etree.ElementTree as ET

import mysql.connector as mycon
import psycopg2 as mycop

import pandas as pd

def get_linkList(junctionSCN, htms_cursor, dbtype):

    linkList = []
    if dbtype == "mysql":

        htms_cursor.execute(
            "SELECT LinkID,LinkOrder FROM utmc_traffic_signal_static_links WHERE SignalSCN='"+str(junctionSCN)+"';")
        raw_data_1 = htms_cursor.fetchall()
        # print(raw_data_1)
        htms_cursor.execute(
            "SELECT TransportLinkRef,SystemCodeNumber FROM utmc_transport_link_static ;")
        raw_data = htms_cursor.fetchall()
        # print(raw_data)
    elif dbtype == "postgresql":
        atuple = (junctionSCN,)
        htms_cursor.execute(
            'SELECT "LinkID","LinkOrder" FROM utmc_traffic_signal_static_links WHERE "SignalSCN"= %s;', atuple)
        raw_data_1 = htms_cursor.fetchall()
        # print(raw_data_1)
        htms_cursor.execute(
            'SELECT "TransportLinkRef","SystemCodeNumber" FROM utmc_transport_link_static ;')
        raw_data = htms_cursor.fetchall()
        # print(raw_data)
    for doublet in raw_data_1:
        linkList.append([int(doublet[0]), int(doublet[1])])
    local_dict = {}

    for doublet in raw_data:
        key = int(doublet[0])
        val = doublet[1]
        local_dict[key] = str(val)

    for doublet in linkList:
        key = doublet[0]
        val = local_dict[key]
        doublet.append(val)
    return linkList


def get_group_settings(groupSCN, htms_cursor, dbtype):

    # gettings junctionSCNs in a Group
    junctionId_list = []
    if dbtype == "mysql":
        htms_cursor.execute(
            "SELECT SignalSCN FROM utmc_traffic_signal_static WHERE Group_SCN ='"+groupSCN+"';")
        raw_data = htms_cursor.fetchall()
    elif dbtype == "postgresql":
        atuple = (groupSCN,)
        htms_cursor.execute(
            'SELECT "SignalSCN" FROM utmc_traffic_signal_static WHERE "Group_SCN" =%s;', atuple)
        raw_data = htms_cursor.fetchall()

    for singlet in raw_data:
        if singlet[0] not in junctionId_list:
            junctionId_list.append(singlet[0])

    # setting up site Id list
    siteId_list = []
    numDets_list = []
    for jun in junctionId_list:
        site = int(jun[1:])
        siteId_list.append(site)
        numDets_list.append(8)  # have to update

    # get link list
    returnDict = {}
    for junction in junctionId_list:
        returnDict[junction] = get_linkList(junction, htms_cursor, dbtype)

    return returnDict
# -------------------------------------------------------------


def junctionToCorridor(htms_cursor, dbtype):
    returnDict = {}
    if dbtype == "mysql":
        htms_cursor.execute("SELECT SignalSCN, CorridorSCN FROM corridors;")
        raw_data = htms_cursor.fetchall()
    elif dbtype == "postgresql":
        htms_cursor.execute(
            'SELECT "SignalSCN", "CorridorSCN" FROM corridors;')
        raw_data = htms_cursor.fetchall()

    for doublet in raw_data:

        if len(doublet) == 2:
            returnDict[doublet[0]] = doublet[1]
        else:
            returnDict[doublet[0]] = "x"
    return returnDict


def junctionToSiteId(htms_cursor, dbtype):
    returnDict = {}
    if dbtype == "mysql":

        htms_cursor.execute(
            "SELECT SignalSCN, site_id FROM utmc_traffic_signal_static;")
        raw_data = htms_cursor.fetchall()
    elif dbtype == "postgresql":
        htms_cursor.execute(
            'SELECT "SignalSCN", "site_id" FROM utmc_traffic_signal_static;')
        raw_data = htms_cursor.fetchall()

    for doublet in raw_data:
        returnDict[doublet[0]] = doublet[1]
    return returnDict


def linkToNtwrkPathRef(htms_cursor, dbtype):
    returnDict = {}
    if dbtype == "mysql":

        htms_cursor.execute(
            "SELECT SystemCodeNumber, NetworkPathReference FROM utmc_transport_link_static;")
        raw_data = htms_cursor.fetchall()
    elif dbtype == "postgresql":
        htms_cursor.execute(
            'SELECT "SystemCodeNumber", "NetworkPathReference" FROM utmc_transport_link_static;')
        raw_data = htms_cursor.fetchall()
    for doublet in raw_data:
        returnDict[doublet[0]] = doublet[1]
    return returnDict


def junctionToDets(htms_cursor, dbtype, preFix=""):
    returnDict = {}
    if dbtype == "mysql":
        htms_cursor.execute(
            "SELECT SystemCodeNumber FROM utmc_detector_static;")
        raw_data = htms_cursor.fetchall()
    elif dbtype == "postgresql":
        htms_cursor.execute(
            'SELECT "SystemCodeNumber" FROM utmc_detector_static;')
        raw_data = htms_cursor.fetchall()
    for singlet in raw_data:
        val = singlet[0]
        if val[0:4] not in returnDict:
            returnDict[val[0:4]] = []
            returnDict[val[0:4]].append(preFix+val)
        else:
            returnDict[val[0:4]].append(preFix+val)
    return returnDict


def updateNtwrkPathRef(afile):
    noMatch = []
    LtoE = linkToNtwrkPathRef()
    df = pd.read_csv(afile)
    df = df.set_index("linkSCN")
    for link in df.index:
        if link in LtoE:
            if df.loc[link, "sumoEdgeID"] == "x":
                df.loc[link, "sumoEdgeID"] = LtoE[link]
        else:
            noMatch.append(link)

    df.to_csv(afile)

    if len(noMatch) > 0:
        return("Couldn't Update For {} As Correspondence Not Found In utmc_transport_link_static.".format(noMatch))
    else:
        return("All Network Path Refs Updated.")


def get_junction_to_link_ord_sat_flow(surveryfile, numLanes, junction, linkOrd):
    ''' Function to read satflow in v/he from survey file if one exists.'''

    df = pd.read_csv(surveryfile, usecols=["ID", "Approach", "satpcu"])
    df = df.ffill()
    mask = ((df["ID"] == junction) & (df["Approach"] == linkOrd))
    df = df.loc[mask]
    if df.shape[0] > 0:
        satflow = df["satpcu"].iloc[0]
        satflow = int(round(0.60 * satflow * 2 + 0.35 *
                            satflow + 0.05 * satflow * 2, 0))
        satflow = satflow * numLanes
        return (True, satflow)
    else:
        return (False, 0)


def updateCityLinker(linkerCSV, groups, htms_cursor, dbtype, surveryfile=None):
    emptyGroups = []

    #df = pd.read_csv(linkerCSV)
    columns = ['linkSCN', 'junctionSCN', 'sumoEdgeID', 'defaultSatFlow', 'detIds', 'detRealIds',
               'linkRef', 'linkOrder', 'phaseNum', 'numPhases', 'GroupSCN', 'CorridorSCN', 'SiteID', 'NumLanes']
    df = pd.DataFrame(columns=columns)
    df = df.set_index("linkSCN")

    JtoC = junctionToCorridor(htms_cursor, dbtype)
    JtoS = junctionToSiteId(htms_cursor, dbtype)
    linkToEdge = linkToNtwrkPathRef(htms_cursor, dbtype)
    JtoD = junctionToDets(htms_cursor, dbtype, preFix="e1det_")
    JtoRD = junctionToDets(htms_cursor, dbtype)

    for group in groups:
        juncsDict = get_group_settings(group, htms_cursor, dbtype)

        if len(juncsDict) < 1:
            emptyGroups.append(group)

        for junction in juncsDict:

            try:
                corridorSCN = JtoC[junction]
            except:
                corridorSCN = "x"
            siteId = JtoS[junction]

            numPhases = 0

            for triplet in juncsDict[junction]:
                tlRef = triplet[0]
                linkOrd = triplet[1]
                numPhases += 1
                phaseNum = (int(linkOrd)-1)*2
                linkSCN = triplet[2]

                # try: # get det ids if there are any
                #sumoEdge =linkToEdge[linkSCN]
                sumoEdge = junction+"_"+"L0"+str(linkOrd)
                edgeDets = []
                realDets = []

                if junction in JtoD:

                    for det in JtoD[junction]:
                        if sumoEdge in det:
                            edgeDets.append(det)
                    for det in JtoRD[junction]:
                        if sumoEdge in det:
                            realDets.append(det)

                    detIds = ",".join(edgeDets)
                    detRealIds = ",".join(realDets)
                    numLanes = len(edgeDets)
                    if numLanes == 0:
                        detIds = "x,x"
                        detRealIds = "x,x"
                        numLanes = 2
                else:
                    detIds = "x,x"
                    detRealIds = "x,x"
                    numLanes = 2

                if surveryfile != None:
                    (sat_bool, sat_pcu) = get_junction_to_link_ord_sat_flow(
                        surveryfile, numLanes, junction, linkOrd)
                    if sat_bool == True:
                        sat_flow = sat_pcu
                    else:
                        sat_flow = numLanes*2350  # 2350 v/hr per lane default sat flow
                else:
                    sat_flow = numLanes*2350  # 2350 v/hr per lane default sat flow

                if linkSCN not in df.index:
                    df.loc[linkSCN] = {"linkRef": tlRef,
                                       "linkOrder": linkOrd,
                                       "GroupSCN": group,
                                       "sumoEdgeID": sumoEdge,
                                       "NumLanes": numLanes,
                                       "CorridorSCN": corridorSCN,
                                       "SiteID": siteId,
                                       "phaseNum": phaseNum,
                                       "defaultSatFlow": sat_flow,
                                       "detIds": detIds,
                                       "detRealIds": detRealIds,
                                       "junctionSCN": junction,
                                       "numPhases": 4}
            # adding num of phases#
            for linkSCN in df.index:
                if df.loc[linkSCN, "junctionSCN"] == junction:
                    df.loc[linkSCN, "numPhases"] = numPhases
    
    df.to_csv(linkerCSV, index_label = 'linkSCN')

    if len(emptyGroups) > 0:
        return("Couldn't Update For Groups:{} As There Is No Corresponding Data In utmc_traffic_signal_static".format(emptyGroups))

    else:
        return("Updated For All Groups Given.")


def fullDataGroups(linker):

    flist = []
    elist = []

    df = pd.read_csv(linker)
    df = df.set_index("linkSCN")

    for link in df.index:
        if df.loc[link, "sumoEdgeID"] == "x":
            if df.loc[link, "GroupSCN"] not in elist:
                elist.append(df.loc[link, "GroupSCN"])
        else:
            if df.loc[link, "GroupSCN"] not in flist:
                flist.append(df.loc[link, "GroupSCN"])

    for grp in elist:
        if grp in flist:
            flist.remove(grp)
    return flist


def fullDataCorridors(linker):

    flist = []
    elist = []

    df = pd.read_csv(linker)
    df = df.set_index("linkSCN")

    for link in df.index:
        if df.loc[link, "sumoEdgeID"] == "x":
            if df.loc[link, "CorridorSCN"] not in elist:
                elist.append(df.loc[link, "CorridorSCN"])
        else:
            if df.loc[link, "CorridorSCN"] not in flist:
                flist.append(df.loc[link, "CorridorSCN"])

    for grp in elist:
        if grp in flist:
            flist.remove(grp)
    return flist


def add_defaults(junctionSCN, linker_file):
    '''
        Adds default linkOrder, phaseNum to a junction
    '''
    df = pd.read_csv(linker_file)
    df = df.set_index("linkSCN")
    linkOrder = 1
    phaseNum = 0
    for link in df.index:
        if df.loc[link, "junctionSCN"] == junctionSCN:
            df.loc[link, "linkOrder"] = linkOrder
            df.loc[link, "phaseNum"] = phaseNum
            linkOrder += 1
            phaseNum += 2
    df.to_csv(linker_file, index=False)


def run_linker(dbCreds, file_name, num_groups, dbtype, surveryfile):
    # DB Connection-----------------------------------------------
    if dbtype == "mysql":
        htmsdb = mycon.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3],port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
    elif dbtype == "postgresql":
        htmsdb = mycop.connect(
            host=dbCreds[0], user=dbCreds[1], password=dbCreds[2], database=dbCreds[3], port=dbCreds[4])
        htms_cursor = htmsdb.cursor()
    # -----------------------------------------------------------

    groups = []
    for i in range(1, int(num_groups)+1):
        group = "GRP0"+"{:02d}".format(i)
        groups.append(group)

    print(updateCityLinker(file_name, groups, htms_cursor, dbtype, surveryfile))

    htms_cursor.close()
    htmsdb.close()

if __name__=="__main__":
    pass
