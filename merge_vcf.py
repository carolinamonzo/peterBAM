#! usr/bin/python3.6

# 2018-07-17, Carolina Monzo

# Variant calling using both samtools and freebayes

import os
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
    parser = argparse.ArgumentParser(description = "Call variants from merged bam file")

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

    #os.chdir(project_path)
    for file in glob.glob(project_path + "config_*.json"):
        config_list.append(file)

    config_pwd = project_path + sorted(config_list)[-1]

    with open(config_pwd, "r") as jconfig:
        config = json.load(jconfig)

    return(config)

def read_input_fof(config):
    """
    Get fof file with information on paths of the merged bam files
    Input: config dictionary with paths to look for the fof file
    Output: list with samples
    """

    # Get fof file
    fof_list = []

    for file in glob.glob(config["paths"]["fof_files"] + "variant_calling_*.fof"):
        fof_list.append(file)

    fof_pwd = os.path.normpath(sorted(fof_list)[-1])

    # Read fof file
    with open(fof_pwd, "r") as fi:
        fof = fi.read().splitlines()

    return(fof)


def cmd_merge_vcf(config, fof):
    """
    Function to get the merged bam files per chromosome with all individuals
    Input: config dictionary with path to files
    Output: cmd file
    """
    cmd_sh = datetime.datetime.now().strftime("cmd_merge_vcf_%Y%m%d_%H-%M-%S.sh")

    with open(config["paths"]["cmd_files"] + cmd_sh, "a") as cmd_file:
        for vcf in fof:
            if vcf.endswith("freebayes.vcf.gz"):
                vcf_name = vcf.split("/")[-1].split("-")[0]
                cmd_str = "java -jar /software/bin/picard.jar MergeVcfs INPUT={}/{}-freebayes.vcf.gz INPUT={}/{}-pileup.vcf.gz OUTPUT={}/{}-merged.vcf.gz; bcftools norm -d both {}/{}-merged.vcf.gz > {}/{}-merged-sorted.vcf.gz".format(config["paths"]["variant_calling"], vcf_name, config["paths"]["variant_calling"], vcf_name, config["paths"]["variant_calling_merged"], vcf_name, config["paths"]["variant_calling_merged"], vcf_name, config["paths"]["variant_calling_merged"], vcf_name)

                cmd_file.write(cmd_str + "\n")

    print("[INFO]: CMD_FILE - {}{}".format(config["paths"]["cmd_files"], cmd_sh))

    return(cmd_sh)

def run_parallel(config, cmd_sh):
    '''

    '''

    log_str = datetime.datetime.now().strftime("merge_vcf_%Y%m%d_%H-%M-%S.log")

    cmd = "parallel --joblog {}/{} -j 18 :::: {}/{}".format(config["paths"]["variant_calling_merged"], log_str, config["paths"]["cmd_files"], cmd_sh)

    print("[CMD] " + cmd)

    # Execute command

    #subprocess.call(cmd + " 2> /dev/null", shell = True)
    subprocess.call(cmd, shell=True)


def write_output_fof(config):
    '''

    '''

    fof = datetime.datetime.now().strftime("merged_vcf_%Y%m%d_%H-%M-%S.fof")
    # Create fof file

    cmd_fof = 'find {} -name "*-merged-sorted.vcf.gz" > {}{}'.format(config["paths"]["variant_calling_merged"], config["paths"]["fof_files"], fof)

    # Write command on the command line

    print("[CMD] " + cmd_fof)
    print("[INFO]: FOF_FILE - {}{}".format(config["paths"]["fof_files"], fof))

    subprocess.call(cmd_fof, shell = True)


def main():

    args = parseArguments()

    config = get_config(args.project_path)

    fof = read_input_fof(config)

    cmd_sh = cmd_merge_vcf(config, fof)

    run_parallel(config, cmd_sh)

    write_output_fof(config)

if __name__ == '__main__':
    main()


