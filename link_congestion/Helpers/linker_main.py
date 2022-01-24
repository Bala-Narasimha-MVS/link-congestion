from link_congestion.Helpers import linker
import configparser

configfile = 'config.ini'

def run_linker_main():
    # Config Parser Settings ----------------------------------
    config = configparser.ConfigParser()
    config.read(configfile)

    dbtype = config["DATABASE"]["dbtype"]
    dbCreds = [config["DATABASE"]["hostname"], config["DATABASE"]["username"], config["DATABASE"]
            ["password"], config["DATABASE"]["database"], int(config["DATABASE"]["port"])]
    file_name = config["FILES"]["linker"]
    num_groups = config["GROUPS"]["number"]

    if "surveyfile" in config["FILES"]:
        survey_file = config["FILES"]["surveyfile"]
    else:
        survey_file = None

    linker.run_linker(dbCreds, file_name, num_groups, dbtype, survey_file)

if __name__ =="__main__":
    run_linker_main()
