#! usr/bin/python3.6

# 2018-07-06, Carolina Monzo

# Mark duplicates on the subsetted bam

import os
import pandas as pd
import re
import argparse
import json
import datetime
import glob
import subprocess

def parseArguments():
    '''
    Function to parse arguments
    Input: path to project
    Output: parsed arguments from command line
    '''

    # Create argument parser class
    parser = argparse.ArgumentParser(description = "Mark duplicates")

    # Define arguments to parse
    parser.add_argument("--project_path", "-p", required = False, type = str, help = "Argument to set the path to the project")

    # Call for arguments
    args = parser.parse_args()

    return(args)

def get_config(project_path):
    """
    Load config dictionary
    """

    config_list = []

    os.chdir(project_path)
    for file in glob.glob("config_*.json"):
        config_list.append(file)

    config_pwd = project_path + sorted(config_list)[-1]

    with open(config_pwd, "r") as jconfig:
        config = json.load(jconfig)

    return(config)

def read_input_fof(config):
    '''
    Function to get merged fastq files fof
    Input: path to the project of interest
    Output: fof file to do the mapping
    '''
    # Get fof file

    fof_list = []

    for file in glob.glob(config["paths"]["fof_files"] + "subset_roi_bam_*.fof"):
        fof_list.append(file)

    fof_pwd = os.path.normpath(sorted(fof_list)[-1])

    # Read fof file

    with open(fof_pwd, "r") as fi:
        fof = fi.read().splitlines()
    return(fof)

def cmd_mark_duplicates(config, fof):
    '''

    '''

    cmd_sh = datetime.datetime.now().strftime("cmd_mark_duplicates_%Y%m%d_%H-%M-%S.sh")

    with open(config["paths"]["cmd_files"] + cmd_sh, "a") as cmd_file:

        for fi in fof:
            sample_name = fi.split("/")[-1].split("_")[0]
            bam_name = fi.split("/")[-1]
            cmd_str = "java -jar /software/bin/picard.jar MarkDuplicates I={} O={}{}_sorted_mkdup.bam ASSUME_SORTED=TRUE REMOVE_DUPLICATES=FALSE CREATE_INDEX=TRUE M={}{}_duplicate_metrics.txt VALIDATION_STRINGENCY=LENIENT 2> {}{}_RemoveDuplicates.err".format(fi, config["paths"]["mapping_roi_mkdup"], sample_name, config["paths"]["mapping_QC"], sample_name, config["paths"]["mapping_roi_mkdup"], sample_name)

            cmd_file.write(cmd_str + '\n')

    print('[INFO]: CMD_FILE - {}{}'.format(config['paths']['cmd_files'], cmd_sh))

    return(cmd_sh)

def run_parallel(config, cmd_sh):
    '''

    '''

    log_str = datetime.datetime.now().strftime("mark_duplicates_%Y%m%d_%H-%M-%S.log")

    cmd = "parallel --joblog {}{} -j 4 :::: {}{}".format(config["paths"]["mapping_roi_mkdup"], log_str, config["paths"]["cmd_files"], cmd_sh)

    print("[CMD]: " + cmd)

    subprocess.call(cmd + " 2> /dev/null", shell = True)

def write_output_fof(config):
    '''

    '''

    fof = datetime.datetime.now().strftime("marked_duplicates_bam_%Y%m%d_%H-%M-%S.fof")

    cmd_fof = 'find {} -name "*_mkdup.bam" > {}{}'.format(config["paths"]["mapping_roi_mkdup"], config["paths"]["fof_files"], fof) 

    print("[CMD]: " + cmd_fof)
    print("[INFO]: FOF_FILE - {}{}".format(config["paths"]["fof_files"], fof))

    # Execute command

    os.system(cmd_fof)



def main():
    '''

    '''

    args = parseArguments()

    config = get_config(args.project_path)

    fof = read_input_fof(config)

    cmd_sh = cmd_mark_duplicates(config, fof)

    run_parallel(config, cmd_sh)

    write_output_fof(config)




if __name__ == '__main__':
    main()
            





