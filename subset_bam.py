#! usr/bin/python3.5

# 2018-07-06, Carolina Monzo

# Subset bam files for ROI on Deepseq - peterBAM

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
    parser = argparse.ArgumentParser(description = "Subset bam for regions of interest")

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
    Function to get fof
    Input: path to the project of interest
    Output: fof file to do the subsetting
    '''
    # Get fof file

    fof_list = []

    for file in glob.glob(config["paths"]["fof_files"] + "mapped_sorted_bwa_mem_*.fof"):
        fof_list.append(file)

    fof_pwd = os.path.normpath(sorted(fof_list)[-1])

    # Read fof file

    with open(fof_pwd, "r") as fi:
        fof = fi.read().splitlines()
    return(fof)

def cmd_subset_bam(config, fof):
    '''
    Function to subset regions of interest for our bam
    Input: list of files to subset
    Output: command for subsetting the bam files
    '''

    cmd_sh = datetime.datetime.now().strftime("cmd_subset_roi_%Y%m%d_%H-%M-%S.sh")

    with open(config["paths"]["cmd_files"] + cmd_sh, "a") as cmd_file:

        for fi in fof:
            sample_name = fi.split("/")[-1].split("_")[0]
            cmd_str = "bedtools intersect -f 0.2 -wa -a {} -b {} | samtools sort -O bam -o {}{}_sorted_subset.bam".format(fi, config["global_config"]["ori_bed"], config["paths"]["mapping_roi"], sample_name)

            cmd_file.write(cmd_str + '\n')

    print('[INFO]: CMD_FILE - {}{}'.format(config['paths']['cmd_files'], cmd_sh))

    return(cmd_sh)


def run_parallel(config, cmd_sh):
    '''

    '''

    log_str = datetime.datetime.now().strftime("subset_roi_%Y%m%d_%H-%M-%S.log")

    cmd = "parallel --joblog {}{} -j 18 :::: {}{}".format(config["paths"]["mapping_roi"], log_str, config["paths"]["cmd_files"], cmd_sh)

    print("[CMD]: " + cmd)

    subprocess.call(cmd + " 2> /dev/null", shell = True)


def write_output_fof(config):
    '''

    '''

    fof = datetime.datetime.now().strftime("subset_roi_bam_%Y%m%d_%H-%M-%S.fof")

    cmd_fof = 'find {} -name "*subset.bam" > {}{}'.format(config["paths"]["mapping_roi"], config["paths"]["fof_files"], fof)

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

    cmd_sh = cmd_subset_bam(config, fof)

    run_parallel(config, cmd_sh)

    write_output_fof(config)



if __name__ == '__main__':
    main()
