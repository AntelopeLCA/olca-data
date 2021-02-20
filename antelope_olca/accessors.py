"""
This class replicates the CSV-loading operations written in spreadsheets.py in a package,
"""
import csv
import os
import re


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
REF_DIR = os.path.join(BASE_DIR, 'refdata')
LCIA_DIR = os.path.join(BASE_DIR, 'LCIA method', 'categories')


def _ref_file(filename):
    return os.path.join(REF_DIR, filename)


def _impact_file(filename):
    return os.path.join(LCIA_DIR, filename)


def impact_methods(filt=None, inverted=False):
    """
    Generate available impact methods, optionally filtering by case-insensitive regex
    :param filt: [None] case-insensitive regex (god help us all if your selection of LCIA methods is case dependent)
    :param inverted: [False] whether to invert the regex test
    :return:
    """
    inverted = bool(inverted)
    if filt:
        _filt = re.compile(filt, flags=re.I)
    else:
        _filt = None

    for cc in os.listdir(LCIA_DIR):
        if _filt:
            f = bool(_filt.search(cc))
        else:
            f = True
        if f ^ inverted:
            yield cc


'''
def _to_dict(ll, nn):
    """
    If for some reason you want fancy keys instead of simple keys
    :param ll:
    :param nn:
    :return:
    """
    dig = ceil(log10(len(ll)))
    return {'%s_%0*d' %(nn, dig, i): k for i, k in enumerate(ll)}
'''


def _read_headless_csv(ref_file, fieldnames=None, delimiter=';'):
    """
    Returns a list of dicts with the specified fieldnames. If no fieldnames are specified,
    returns a list of dicts whose keys are the column indices (i.e. works just like a list)
    :param ref_file:
    :param fieldnames:
    :param delimiter:
    :return:
    """
    with open(_ref_file(ref_file)) as fp:
        if fieldnames is None:
            clr = csv.reader(fp, delimiter=delimiter)
            return [{i: k for i, k in enumerate(ll)} for ll in clr]
        else:
            cdr = csv.DictReader(fp, fieldnames=fieldnames, delimiter=delimiter)
            return list(cdr)

'''
You know what would eliminate the need for this dict?
IF THE CSV FILES HAD A HEADER ROW!
'''

FIELD_NAMES = {
    'categories': ('UUID', 'CATEGORY_NAME', "CATEGORY_2", "TYPE", "PARENT"),
    'currencies': ('UUID', 'CURRENCY_NAME', "DESCRIPTION", "CURRENCY_3", "CURRENCY_4", "UNIT", "VALUE"),
    'flows': ('UUID', 'FLOW_NAME', "FLOW_2", "CAT_UUID", "FLOW_TYPE", "CAS_NUMBER", "FORMULA", "REF_QTY"),
    'flow_properties': ("UUID", "FLOW_PROPERTY_NAME", "FLOW_PROPERTY_2","CAT_UUID","UNIT_GROUP","IS_PHYSICAL"),
    'flow_property_factors': ("FLOW_UUID", "FLOW_PROPERTY_UUID", "VALUE"),
    'locations': ("UUID", "LOCATION_NAME", "DESCRIPTION", "SHORT_LOCALE", "LATITUDE", "LONGITUDE"),
    'unit_groups': ("UUID", "UNIT_GROUP_NAME", "UNIT_GROUP_2", "CAT_UUID", "UNIT_GROUP_4", "REF_UNIT_UUID"),
    'units': ("UUID", "UNIT_STRING", "UNIT_NAME", "FACTOR", "SYNONYMS", "UNIT_GROUP_UUID")
}


def read_refdata(file):
    fieldnames = FIELD_NAMES[file]
    return _read_headless_csv(file + '.csv', fieldnames=fieldnames)


def read_contexts():  #  -> Dict[str: tuple]
    cats = read_refdata('categories')
    catdir = {cat['UUID']: cat for cat in cats}

    contexts = {}
    for cat in cats:
        cx = [cat['CATEGORY_NAME']]
        parent = catdir.get(cat["PARENT"])
        while parent is not None:
            cx.append(parent['CATEGORY_NAME'])
            parent = catdir.get(parent["PARENT"])

        contexts[cat[0]] = tuple(cx[::-1])  # reverse order

    return contexts
