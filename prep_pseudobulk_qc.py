import yaml
from wgs.utils import helpers
import os

def addlumpy(yam):

    new = {}
    for k, v in yam.items():
        
        if isinstance(v, dict):
            v = addlumpy(v)
        if k == "alignment_metrics":
           label =v.split("/")[5]
        if k == "breakpoint_annotation":
            new["destruct_breakpoint_annotation"] = v
            new["lumpy_breakpoint_annotation"] = os.path.join(os.path.dirname(v), "lumpy_breakpoints.csv.gz")
        if k == "breakpoint_counts":
            new["destruct_breakpoint_counts"] = v    
            new["lumpy_breakpoint_evidence"] = os.path.join(os.path.dirname(v), "lumpy_breakpoints_evidence.csv.gz")
        else:
            new[k] = v
    return new


def delete_keys_from_dict(dict_del, lst_keys):
    """
    Delete the keys present in lst_keys from the dictionary.
    Loops recursively over nested dictionaries.
    """
    dict_foo = dict_del.copy()  #Used as iterator to avoid the 'DictionaryHasChanged' error
    for field in dict_foo.keys():
        if field in lst_keys:
            del dict_del[field]
        if type(dict_foo[field]) == dict:
            delete_keys_from_dict(dict_del[field], lst_keys)
    return dict_del

yam = helpers.load_yaml("pseudobulkqc.yaml")


new = addlumpy(yam)
new = delete_keys_from_dict(new, ["breakpoint_annotation", "breakpoint_counts"])


print(len(new.keys()))
for k, v in new.items():
    print(k, "\n", v, "\n\n\n\n\n")
    out = {k: v}
    print(out, "\n\n\n\n\n")
    with open('yamls/pseudobulkqc_{}.yaml'.format(k), 'w') as outfile:
        yaml.dump(out, outfile, default_flow_style=False)
