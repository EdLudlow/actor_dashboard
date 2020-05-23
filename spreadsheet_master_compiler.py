import glob
import os
import xlrd
import pandas as pd
import numpy as np
import re

 ########### GLOBAL VARIABLES ############

OUTPUT_DIRECTORY = '/Users/EdmundLudlow/Desktop/Programming/actor_dashboard/bfi_spreadsheets/spreadsheet_directory/'
LIST_OF_DATAFRAMES = []

def create_filename_list(list_of_years):
    xls_filename_list = []
    for file_name in glob.glob(OUTPUT_DIRECTORY + '*'):
        if any(x in file_name for x in list_of_years):
            xls_filename_list.append(file_name)
    return xls_filename_list

def drop_excess_columns(dataframe, columns_to_drop):
    for index_name in columns_to_drop:
            if index_name in dataframe.columns:
                dataframe.drop(columns = [index_name], axis=1, inplace=True)
    return dataframe

def format_columns(dataframe):
    dataframe.columns= ['Rank', 'Title', 'Weekend Gross', 'Distributor', 'Weeks on release', 'Total Gross to date']
    dataframe.dropna(subset = ['Rank', 'Total Gross to date'], inplace=True)
    return dataframe

def set_column_index(dataframe):
        new_header = dataframe.iloc[0]
        dataframe = dataframe[1:] 
        dataframe.columns = new_header
        return dataframe

def spellcheck_film(film_title):
    film_title = film_title.strip()
    # if film ends with ', the', trim and add to prefix
    if film_title.endswith(", THE"):
        film_title = "THE " + film_title.rstrip(", THE")
    return film_title

def format_rank(rank):
    if isinstance(rank, str):
        rank = re.sub('=', '', rank)
    return rank


if __name__ == '__main__':
    

    ########### 2001 ############


    xls_2001 = OUTPUT_DIRECTORY + 'UK_weekend_box_office_reports_2001'

    #Grab July and December dataframes
    jul_data_2001 = pd.read_excel(xls_2001, sheet_name= ['Jul', 'Dec'], skiprows = 2)
    jul_df_2001 = pd.concat(jul_data_2001)
    #Grab dataframes for other months
    other_months_data_2001 = pd.read_excel(xls_2001, sheet_name = ['Aug', 'Sept', 'Oct', 'Nov'], skiprows = 3)
    other_months_df_2001 = pd.concat(other_months_data_2001)
    #Combine the two dataframes from 2001
    combined_df_2001 = pd.concat([jul_df_2001, other_months_df_2001])
    combined_df_2001 = drop_excess_columns(combined_df_2001, ['% chg', 'Sites'])

    #Format combined dataframe
    formatted_2001 = format_columns(combined_df_2001)

    #Append dataframes to list
    LIST_OF_DATAFRAMES.append(formatted_2001)


    ########### 2003 ############

    
    xls_2003 = OUTPUT_DIRECTORY + 'UK_weekend_box_office_reports_2003'

    #Grab January data
    jan_data_2003 = pd.read_excel(xls_2003, sheet_name= 'Jan', skiprows = 3)

    #Grab data for other months
    other_months_2003 = ['Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    other_months_data_2003 = pd.read_excel(xls_2003, sheet_name = other_months_2003, skiprows = 2)
    other_months_df_2003 = pd.concat(other_months_data_2003)

    #Filter out excess column from other months dataframe
    other_months_df_2003['Cum to date'] = other_months_df_2003['Cum to date'].fillna(other_months_df_2003['Box office  to date'])
    other_months_df_2003.drop(columns = ['Box office  to date'], inplace = True)


    #Combine the two dataframes from 2003, drop excess columnns and format
    combined_df_2003 = pd.concat([jan_data_2003, other_months_df_2003])
    combined_df_2003 = drop_excess_columns(combined_df_2003, ['% chg', 'Sites'])

    #Format combined dataframe
    formatted_2003 = format_columns(combined_df_2003)
    formatted_2003['Rank'] = formatted_2003['Rank'].apply(format_rank)

    #Append dataframes to list
    LIST_OF_DATAFRAMES.append(formatted_2003)

    
    ########### 2002-2006 ############


    #create a list of fienames to loop through
    list_of_years_2002_2006 = ['2002', '2004', '2005', '2006']
    filename_list_2002_2006 = create_filename_list(list_of_years_2002_2006)

    for xls_file_2002_2006 in filename_list_2002_2006 :
        #concatenate all dataframes from separate sheets
        all_dfs_2002_2006 = pd.read_excel(xls_file_2002_2006, skiprows = 2, sheet_name = None)    
        df_2002_2006 = pd.concat(all_dfs_2002_2006)

        #remove excess columns
        columns_to_drop_2002_2006 = ['Site avg', 'Site Avg', '% chg', 'Sites']
        df_2002_2006 = drop_excess_columns(df_2002_2006, columns_to_drop_2002_2006)


        #Filter out excess Box Office to Date values
        if 'Box office  to date' in df_2002_2006.columns:
            df_2002_2006['Box office  to date'] = df_2002_2006['Box office  to date'].fillna(df_2002_2006['Box office to date'])
            df_2002_2006.drop(columns = ['Box office to date'], inplace = True)

        #Format combined dataframe
        formatted_2002_2006 = format_columns(df_2002_2006)

        #Append dataframes to list
        LIST_OF_DATAFRAMES.append(formatted_2002_2006)

   
    ########### 2007 - 2020 ############


    #create a list of fienames to loop through
    list_of_years_2007_2020 = [str(year) for year in range(2007, 2021)]
    filename_list_2007_2020 = create_filename_list(list_of_years_2007_2020)

    #Loop through files
    for xls_file_2007_2020 in filename_list_2007_2020:
        dfs_2007_2020 = pd.read_excel(xls_file_2007_2020)

        if len(dfs_2007_2020.columns) > 10:
            dfs_2007_2020.drop(dfs_2007_2020.iloc[:, 10:], inplace = True, axis = 1)
        
        #Format dataframes
        #Correct column index line
        if 'Rank' in dfs_2007_2020.columns:
                continue
        else:
            dfs_2007_2020 = set_column_index(dfs_2007_2020)

        if 'Rank' not in dfs_2007_2020.columns:
            dfs_2007_2020 = set_column_index(dfs_2007_2020)
        
        #Drop empty and excess column indices 
        columns_to_drop_2007_2020 = ['Country of Origin', '% change on last week', 'Number of cinemas',
                'Site average','% chg', 'Sites', 'Site Avg', 'CoO', '%', 'Locs', 'Weekend Loc Avg', 'Running  Total',
                'UK=yes', 'BO  of UK films']
        dfs_2007_2020 = drop_excess_columns(dfs_2007_2020, columns_to_drop_2007_2020)
        
        #Format combined dataframe
        formatted_2007_2020 = format_columns(dfs_2007_2020)

        #Append dataframes to list
        LIST_OF_DATAFRAMES.append(formatted_2007_2020)


    ########### COMBINE, CORRECT FILM NAMES, AND WRITE TO FILE ############


    result = pd.concat(LIST_OF_DATAFRAMES)
    result['Title'] = result['Title'].apply(str).apply(spellcheck_film)

    result.to_csv(OUTPUT_DIRECTORY + '/UK_Box_Office_Compiled_Master')
    print('Done')
