#! usr/bin/python3.6

# 2018-04-23, Carolina Monzo

# Variant calling

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

    """

    # Get fof file
    fof_list = []

    for file in glob.glob(config["paths"]["fof_files"] + "variant_annotation*.fof"):
        fof_list.append(file)

    fof_pwd = os.path.normpath(sorted(fof_list)[-1])

    # Read fof file
    with open(fof_pwd, "r") as fi:
        fof = fi.read().splitlines()

    return(fof)

def cmd_preprocess_vcf(config, fof):
    """

    """

    cmd_sh = datetime.datetime.now().strftime("cmd_preprocess_vcf_%Y%m%d_%H-%M-%S.sh")

    vcfs_str = " ".join(fof)

    with open(config["paths"]["cmd_files"] + cmd_sh, "a") as cmd_file:
        cmd_str = "vcf-merge {} | vt decompose -s - | vt normalize -r {} -n - | bgzip -c > {}/preprocessed_deepseq.vcf.gz; tabix -p vcf {}/preprocessed_deepseq.vcf.gz".format(vcfs_str, config["global_config"]["reference_genome"], config["paths"]["variant_annotation"], config["paths"]["variant_annotation"])
        cmd_file.write(cmd_str)

    print("[INFO]: CMD_FILE - {}{}".format(config["paths"]["cmd_files"], cmd_sh))

    return(cmd_sh)

def run_parallel(config, cmd_sh):
    """

    """
    log_str = datetime.datetime.now().strftime("preprocess_vcf_%Y%m%d_%H-%M-%S.log")

    cmd = "parallel --joblog {}{} -j 1 :::: {}{}".format(config["paths"]["variant_annotation"], log_str, config["paths"]["cmd_files"], cmd_sh)

    print("[CMD]: " + cmd)
    
    subprocess.call(cmd + " 2> /dev/null", shell = True)


def main():
    args = parseArguments()

    config = get_config(args.project_path)

    fof = read_input_fof(config)

    cmd_sh = cmd_preprocess_vcf(config, fof)

    run_parallel(config, cmd_sh)

    print("[INFO]: FINAL_FILE - {}{}".format(config["paths"]["variant_annotation"], "preprocessed_deepseq.vcf.gz"))

if __name__ == "__main__":
    main()
