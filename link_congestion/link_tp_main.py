import configparser

from numpy import real
from link_congestion import link_traffic_parameters as ltp
from datetime import datetime, timedelta
import time
import schedule
import sys

#from itspelogger import Logger
# logger=Logger("LTP-MAIN")

configfile = 'config.ini'

def timeit(func):
    '''
        Decorator to measure running time of a function.
    '''
    def timed(*args, **kw):
        print(f"Time Zone : {time.tzname[0]}")
        print(f"Start Time : {time.asctime()}")
        times = time.time()
        result = func(*args, **kw)
        timee = time.time()

        print('%r  %2.2f ms' % (func.__name__, (timee - times) * 1000))
        print(f"End Time : {time.asctime()}")

        return result
    return timed


@timeit
def ltp_main(configfile):

    print("Started At {} ".format(
        datetime.now()))
    print("------------------------------------------")

    config = configparser.ConfigParser()
    config.read(configfile)

    # Settings ##########################################
    junctions = config["JUNCTIONS"]["junctions"]
    if junctions != "ALL":
        if "," not in junctions:
            junctions = [junctions]
        else:
            junctions = junctions.split(",")
    table = config["TABLES"]["LINK_DATA_TABLE"]
    dbcreds = [config["DATABASE"]["hostname"], config["DATABASE"]["username"],
               config["DATABASE"]["password"], config["DATABASE"]["database"], config["DATABASE"]["port"], config["DATABASE"]["dbtype"]]
    linker = config["FILES"]["linker"]
    nMin = int(config["TIMES"]["NMIN"])
    ext_links = int(config["EXTERNAL_LINKS"]["EXT_LINKS"])
    printer = int(config["DATABASE_CHANGES"]["PRINTER"])
    INSERT = int(config["DATABASE_CHANGES"]["INSERT"])
    def_sat = int(config["DEFAULT"]["SAT_FLOW"])
    default_dict = {"linkLength": int(config["DEFAULT"]["LINK_LENGTH"]), "designSpeed": int(
        config["DEFAULT"]["LINK_SPEED"])}
    reality = config['TABLES']['REALITY']
    no_det_link_groups = []  # ["GRP004"]
    #####################################################

    # Code ###############################################
    linkAttribDict = ltp.get_link_attribs(
        linker, def_sat, dbcreds, default_dict, junctions)
    # print('reality', reality)
    if reality in ['Atflo', 'Simulation', 'Prediction']:
        linkCurDict = ltp.get_link_tt(linkAttribDict, nMin, def_sat, dbcreds)
    if reality == ['Simulation', 'Prediction']:
        linkCurDict = ltp.get_estimated(linkCurDict)
    if reality == 'Prediction':
        linkCurDict = ltp.get_predicted(linkCurDict, linkAttribDict, nMin, dbcreds)
    
    
    # adding kpi
    linkCurDict = ltp.add_kpi(linkAttribDict, linkCurDict)

    # no dets groups
    if len(no_det_link_groups) >= 1:
        no_det_links = ltp.no_det_links(linker, no_det_link_groups)
        print("no_det_links : {}".format(no_det_links))
        linkCurDict.update(ltp.create_data_for_no_det_links(no_det_links, reality))

    if ext_links == 1:
        extDict = ltp.get_extension_links(dbcreds)
        linkAttribExtDict = ltp.get_link_attribs_ext_links(linkAttribDict, extDict, def_sat)

        if reality in ['Atflo', 'Simulation', 'Prediction']:
            linkCurExtDict = ltp.get_link_tt_extension(linkCurDict, linkAttribDict, extDict)
        if reality == ['Simulation', 'Prediction']:
            linkCurExtDict = ltp.get_estimated(linkCurExtDict)
        if reality == 'Prediction':
            linkCurExtDict = ltp.get_predicted(linkCurExtDict, linkAttribExtDict, nMin, dbcreds)


    if printer == 1:
        print("LinkCurDict : {}".format(linkCurDict))

        if ext_links == 1:

            print("LinkCurExtDict : {}".format(linkCurExtDict))

    #print("linkTTdict : {}".format(linkTTdict))
    if INSERT == 1:

        ltp.update(dbcreds, reality)
        if printer == 1:
            print(f"Updated {reality}.")

        if ext_links == 1:
            ltp.update_ext(dbcreds, "Detector", "ext")
            if printer == 1:
                print("Updated Detector for ext links.")

        ltp.insert(linkCurDict, table, dbcreds, reality)
        if ext_links == 1:
            ltp.insert(linkCurExtDict, table, dbcreds, reality)
            # ltp.insert(linkCurExtDict, table, dbcreds, "Detector")
        if printer == 1:
            print(f"Inserted {reality}.")

        print("Successfully inserted into {}".format(table))
        # inserting into kpi report table
        ltp.insert_into_kpi_report_table(
            linkCurDict, linkAttribDict, linker, dbcreds)
    ##################### END ############################

    print("Ended At {} ".format((datetime.utcnow() +
                                 timedelta(minutes=30, hours=5)).strftime('%Y-%m-%d %H:%M:%S')))


def scheduler():

    config = configparser.ConfigParser()
    config.read(configfile)

    nMin = int(config["RUN"]["FREQUENCY"])

    schedule.every(nMin).minutes.do(ltp_main, configfile=configfile)

    while True:
        schedule.run_pending()
        time.sleep(1)

def test():
    ltp_main()
    pass

if __name__ == "__main__":
    ltp_main(sys.argv[1])
