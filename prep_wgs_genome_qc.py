

import yaml
from wgs.utils import helpers
import os
import sys
import pandas as pd
import argparse
import pull_isabl_api
import logging
import yaml

# SPECTRUM-OV-014:
#   breakpoints_consensus: /juno/work/shah/isabl_data_lake/analyses/46/89/4689/results/breakpoints/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_filtered_consensus_calls.csv.gz
#   germline_calls: /juno/work/shah/isabl_data_lake/analyses/46/73/4673/results/variants/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_samtools_germline.vcf.gz
#   normal_bam: /juno/work/shah/isabl_data_lake/analyses/24/21/2421/results/SPECTRUM-OV-014_BC1_NORMAL_NORMAL/SPECTRUM-OV-014_BC1_NORMAL_NORMAL.bam
#   remixt: /juno/work/shah/isabl_data_lake/analyses/46/97/4697/results/remixt/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_remixt.h5
#   roh: /juno/work/shah/isabl_data_lake/analyses/46/73/4673/results/variants/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_roh.csv
#   somatic_calls: /juno/work/shah/isabl_data_lake/analyses/46/73/4673/results/variants/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_consensus_somatic.csv.gz
#   titan: /juno/work/shah/isabl_data_lake/analyses/46/81/4681/results/copynumber/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM/titan/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_titan_markers.csv.gz
#   tumour_bam: /juno/work/shah/isabl_data_lake/analyses/46/65/4665/results/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_TUMOR/SPECTRUM-OV-014_S2_RIGHT_ABDOMEN_DIAPHRAGM_TUMOR.bam

result_path = {
   "WGS-VARIANTCALLING" : "results/variants",
   "WGS-BREAKPOINTCALLING" :  "results/breakpoints",
   "WGS-ALIGNMENT" :  "results",
   "WGS-COPYNUMBERCALLING" : "results/copynumber",
   "WGS-REMIXT" : "results/remixt",
}


def make_category_label(sample_group):
    return "normal_group:----{}----tumour_group:----{}".format(*sample_group)


def get_data_in_groups(input_categories, data):
    for label_set in input_categories:
        pattern = '({})|({})'.format(*label_set)
        yield isabl_data[isabl_data.target_sample.str.contains(pat = pattern)]


def get_fuller_path(r):
    sample = r.target_sample
    sample_category = r.target_sample_category
    if r.app == "WGS-ALIGNMENT":
        sample += "_" + sample_category
    if r.app == "WGS-COPYNUMBERCALLING":
        return os.path.join(r.path, result_path[r.app], sample, "titan", sample)
    return os.path.join(r.path, result_path[r.app], sample, sample)


def format_yaml_output(samples, sample_group):
    
    group_as_name = make_category_label(sample_group)

    output_names = [sample + "----INFO:-----" + group_as_name for sample in samples.groups.keys()]


    for (i, data), output_name in zip(samples, output_names):

        out = {}
        if (len(data.index) < 6):
            print('''missing data modalities for current sample. Check for data completeness
            for sample {}, normal group {}, and tumour group {}'''.format(i, *sample_group))
            yield output_name, out
            continue

        tumour = data[data.target_sample_category ==  "TUMOR"]
        normal = data[data.target_sample_category ==  "NORMAL"]

        
        tumour_bam_base = tumour[tumour.app == "WGS-ALIGNMENT"].full_base_path.tolist()
        assert len(tumour_bam_base) == 1

        normal_bam_base = normal[normal.app == "WGS-ALIGNMENT"].full_base_path.tolist()
        assert len(normal_bam_base) == 1

        breakpoint_base = tumour[tumour.app == "WGS-BREAKPOINTCALLING"].full_base_path.tolist()
        assert len(breakpoint_base) == 1

        cn_base = tumour[tumour.app == "WGS-COPYNUMBERCALLING"].full_base_path.tolist()
        assert len(cn_base) == 1

        variant_base = tumour[tumour.app == "WGS-VARIANTCALLING"].full_base_path.tolist()
        assert len(variant_base) == 1

        remixt_base = tumour[tumour.app == "WGS-REMIXT"].full_base_path.tolist()
        assert len(remixt_base) == 1


        out["breakpoints_consensus"] = breakpoint_base[0] + "_filtered_consensus_calls.csv.gz"
        out["germline_calls"] = variant_base[0] +  "_samtools_germline.vcf.gz"
        out["normal_bam"] = normal_bam_base[0] + ".bam"
        out["remixt"] = remixt_base[0] + "_remixt.h5"
        out["roh"] = variant_base[0] + "_roh.csv"
        out["somatic_calls"] = variant_base[0] + "_consensus_somatic.csv.gz"
        out["titan"] = cn_base[0] + "_titan_markers.csv.gz"
        out["tumour_bam"] = tumour_bam_base[0] + ".bam"

        assert os.path.exists(out["breakpoints_consensus"])
        assert os.path.exists(out["germline_calls"])
        assert os.path.exists(out["normal_bam"])
        assert os.path.exists(out["remixt"])
        assert os.path.exists(out["roh"])
        assert os.path.exists(out["somatic_calls"])
        assert os.path.exists(out["titan"])
        assert os.path.exists(out["tumour_bam"])

        yield output_name, out



parser = argparse.ArgumentParser()
parser.add_argument('--project_name', help='foo help')
parser.add_argument('--normal_sample_types', nargs='+', type=str)
parser.add_argument('--tumour_sample_types', nargs='+', type=str)
parser.add_argument('--output_dirname', help='foo help')
parser.add_argument('--sample_level_yamls', help='foo help')


args = parser.parse_args()

normal_labels = args.normal_sample_types
tumour_labels = args.tumour_sample_types
project = args.project_name
output_dir = args.output_dirname
sample_level_yamls = bool(args.sample_level_yamls)

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

input_categories = [( norm_type,  tum_type ) for norm_type in normal_labels
     for tum_type in tumour_labels]


relavent_apps = "WGS-REMIXT, WGS-COPYNUMBERCALLING, WGS-VARIANTCALLING, WGS-BREAKPOINTCALLING, WGS-ALIGNMENT"

isabl_data = pull_isabl_api.get_data(relavent_apps, project)

isabl_data["full_base_path"] = isabl_data.apply(lambda r: get_fuller_path(r), axis=1)

print("\nproducing {} input yamls: input type categories: {}\n".format(len(input_categories), input_categories))


for data_group, category in zip(get_data_in_groups(input_categories, isabl_data), input_categories):

    samples = data_group.groupby("individual")

    for output_group, prepped_sample_output in format_yaml_output(samples, category):
        
        if prepped_sample_output != {}:
            category_label = make_category_label(category)
            if sample_level_yamls: 
                output_name = output_group + ".yaml"
                write_dir = os.path.join(output_dir, category_label)
                if not os.path.exists(write_dir):
                    os.makedirs(write_dir)
            else:
                output_name =category_label + ".yaml"
                write_dir = output_dir
            
            with open(os.path.join(write_dir, output_name), 'w') as outfile:
                yaml.dump({output_group: prepped_sample_output}, outfile, default_flow_style=False)
        
