import pandas as pd

def get_primary_key_fields(t):
    """
    Get list of all fields that compose primary key
    Args:
        t (Dj table): Instance of a table in datajoint 
    Returns:
        primary_field_list: (list): List of all fields that make primary key
    """

    fields_t = pd.DataFrame.from_dict(t.heading.attributes, orient='index')
    primary_field_list = fields_t.loc[fields_t['in_key'] == True].index.to_list()
    
    return primary_field_list


# Function copied in dj_shorts
def smart_dj_join(t1, t2):
    """
    Join two datajoint tables even if they have matching secondary field names
    Matching secondary fields from t2 will be rewritten as 'field -> t2.table_name+"_"+field'
    """

    # Get all fields from tables
    fields_t1 = pd.DataFrame.from_dict(t1.heading.attributes, orient='index')
    fields_t2 = pd.DataFrame.from_dict(t2.heading.attributes, orient='index')

    # Get only secondary fields and check matches
    fields_t1_list = set(fields_t1.loc[fields_t1['in_key'] == False].index.to_list())
    fields_t2_list = set(fields_t2.loc[fields_t2['in_key'] == False].index.to_list())
    intersected_fields = fields_t2_list.intersection(fields_t1_list)

    # If there are:
    if len(intersected_fields) > 0:
        # Create a dictionary to rename matching ones
        suffix = t2.table_name
        new_name_attr_dict = dict()
        for i in intersected_fields:
            new_name_attr_dict[suffix + '_' + i] = i

        # List non matching ones
        non_intersected_fields = list(fields_t2_list - intersected_fields)

        # Finally merge
        t = t1 * t2.proj(*non_intersected_fields, **new_name_attr_dict)
    # If there are not, normal merge
    else:
        t = t1 * t2

    return t


def get_string_key(key):
    """
    Translate list or dict key to string
    """

    str_key = ''
    if isinstance(key, list):
        str_key = [[k + '=' + str(v) for k,v in x.items()] for x in key]
        str_key = ['('+' and '.join(sublist)+')' for sublist in str_key]
        str_key = ' or '.join(str_key)
    elif isinstance(key, dict):
        str_key = [k + '=' + str(v) for k,v in key.items()] 
        str_key = ' and '.join(str_key)
    elif isinstance(key,str):
        str_key = key
    
    return str_key