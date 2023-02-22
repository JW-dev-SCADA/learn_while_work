
import pandas as pd
from datetime import datetime
import copy
import csv

DNP3_TYPE_PAIRS = [
    {
        "Out": "BinaryOutputs",
        "In": "BinaryInputs"
    },
    {
        "Out": "AnalogOutputs",
        "In": "AnalogInputs"
    }
]


def split_dataframe_by_column(df, column_name):
    # get a list of unique values in the specified column
    column_values = df[column_name].unique()

    # split the dataframe into a list of dataframes by the unique values in the specified column
    df_list = [df.loc[df[column_name] == value] for value in column_values]

    return df_list


def update_dataframe_if_match(df1, df2, compare_columns, update_function):
    """
        A function which takes two dataframe of potentially different length but with the same columns, 
        loop through them, comparing each based on a list of specified column names. 
        If they are equal, update the row of the first dataframe according to a passed in function, 
        otherwise print that they were not equal to the console and some other identifying information about the row. 
    """
    modifed_vars = []
    
    for i, row1 in df1.iterrows():
        # get the corresponding row from df2
        row2 = df2.loc[df2[compare_columns].eq(row1[compare_columns]).all(axis=1)]
        if len(row2) == 1:
            # if the rows match, update the row of df1 using the passed-in function
            df1.loc[i] = update_function(row1, row2.iloc[0])
            modifed_vars.append(row2[" Variable Name"].iloc[0])
            
        else:
            # if the rows do not match, print a message to the console
            print("--------------")
            print(f"No entry was found for row {i} in df1 with {compare_columns}: {row1[compare_columns]}")
            print("--------------")
    
    return modifed_vars
            


def update_variable_names(series, series2):
    series[" Variable Name"] = series[" Variable Name"] + "_fb"
    return series


def create_structured_text(strings, output_file=None):

    output_lines = []
    for string in strings:
        if string.startswith('@GV.'):
            string = string[4:]
        output_lines.append(f'{string}:={string}_fb;')
    output_str = '\n'.join(output_lines)
    
    if output_file is not None:
        with open(output_file, 'w') as file:
            file.write(output_str)
    
    return output_str


def create_variable_sheet(strings, lookup_path, output_file=None):
    """
    A function which takes a list of strings and a path to a lookup csv file. Function first looks for "@GV." 
    at the start of each string in the list and removes it if it exists. For each string in the list, function 
    looks up the name in the CSV file in the first column and then looks for the corresponding data type in the 
    second column. Based on this, the function builds a string which contains the "{string}_fb", then a tab and 
    then the data type. A new line is added for each string in the original list and "NOT FOUND" is added for 
    the datatype if not found. 
    """
    # Load the lookup table into a dictionary for faster lookups
    lookup_dict = {}
    with open(lookup_path) as file:
        reader = csv.reader(file, delimiter='\t')
        for row in reader:
            if len(row) >= 2:
                lookup_dict[row[0]] = row[1]

    # Process each string and build the output string
    output_lines = []
    for string in strings:
        if string.startswith('@GV.'):
            string = string[4:]
        datatype = lookup_dict.get(string, 'NOT FOUND')
        output_lines.append(f'{string}_fb,{datatype}')

    # Join the output lines into a single string with newline characters
    output_str = '\n'.join(output_lines)

    if output_file is not None:
        with open(output_file, 'w') as file:
            file.write(output_str)
    
    
    return output_str




if __name__ == "__main__":
    df = pd.read_csv('in/map.csv')

    print(df.head())

    print("... splitting dataframe by DNP3 data type")
    dfs = split_dataframe_by_column(df, "Type")

    # manually pass in the dataframes based on index. Could have some code that works this out
    # but for now this is done manually. 
    # you just need to check the order of types in the CSV is BinaryInputs, BinaryOutputs, AnalogInputs, AnalogOutputs
    bins = copy.deepcopy(dfs[0])
    bouts = copy.deepcopy(dfs[1])
    ains = copy.deepcopy(dfs[2])
    aouts = copy.deepcopy(dfs[3])

    print("... updating dataframes")
    binary_changes = update_dataframe_if_match(bins, bouts, [" DNP3 Address", " Variable Name"], update_variable_names)
    analog_changes = update_dataframe_if_match(ains, aouts, [" DNP3 Address", " Variable Name"], update_variable_names)

    dfs_out = [bins, bouts, ains, aouts]
    df_out = pd.concat(dfs_out, axis=0, ignore_index=True)
    # replace NaN values with an empty string
    df_out = df_out.fillna('')

    # write the modified dataframe to a new CSV file
    # get the current date and time
    now = datetime.now()
    # format the date and time as a string
    formatted = now.strftime("%Y%m%d_%H%M")

    print("... saving scada mapping")
    filename = formatted + "_map_out.csv"
    df_out.to_csv('out/%s' % filename, na_rep='', index=False)

    print("... saving structured text")
    create_structured_text(binary_changes + analog_changes, 'out/' + formatted + "_ST.txt")

    print("... saving variable sheet")
    create_variable_sheet(binary_changes + analog_changes, 'in/var_sheet.csv', 'out/' + formatted + "_var_sheet.csv")