import glob
import os
import xlrd
import pandas as pd
import numpy as np
import re
from datetime import datetime


########### FUNCTIONS ############


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
    dataframe.columns= ['Rank', 'Film', 'Weekend Gross', 'Distributor', 'Weeks on release', 'Number of cinemas', 'Total Gross to date']
    dataframe = dataframe.reindex(columns = dataframe.columns.tolist() + ['Date Column','Release Date', 'Opening Weekend', 'Screen Average'])
    return dataframe

def set_column_index(dataframe, header_row):
        new_header = dataframe.iloc[header_row]
        dataframe = dataframe[header_row+1:] 
        dataframe.columns = new_header
        return dataframe

def spellcheck_film(film_title):
    film_title = film_title.strip()
    # if film ends with ', the', trim and add to prefix
    if film_title.endswith(", THE"):
        film_title = "THE " + film_title.rstrip(", THE")
    return film_title

def apply_float(data_frame):
    try:
        return float(data_frame)
    except ValueError:
        return None


########### GLOBAL VARIABLES ############


OUTPUT_DIRECTORY = '/OUTPUT_DIRECTORY'
LIST_OF_DATAFRAMES = []
MONTHS = {'Jan': 'January',
        'Feb': 'February',
        'Mar': 'March',
        'Apr': 'April',
        'May': 'May',
        'Jun': 'June',
        'Jul': 'July',
        'Aug': 'August',
        'Sep': 'September',
        'Oct': 'October',
        'Nov': 'November',
        'Dec': 'December'}


if __name__ == '__main__':
    
    
    ########### SPLIT EXCEL DOCS AND SETUP ############


    #create a list of fienames to loop through 2008-2020
    list_of_years_2008_2020 = [str(year) for year in range(2008, 2021)]
    filename_list_2007_2020 = create_filename_list(list_of_years_2008_2020)

    #create a list of fienames to loop through 2007 and split off dataframes which don't fit formula
    filename_including_2007 = create_filename_list(['2007'])
    filename_list_2007 = []
    for file_name in filename_including_2007:
        if 'January' in file_name or 'February' in file_name or 'March' in file_name or 'April'in file_name or 'May' in file_name:
            filename_list_2007.append(file_name)
        else: 
            filename_list_2007_2020.append(file_name)
    

    ########### JANUARY, FEBRUARY, MARCH, APRIL, MAY 2007 ############


    jan_feb_mar_apr_may_2007 = []
    row_indices =[]
    dataframes_2001_2007 =[]
    
    for xls_file_2007 in filename_list_2007:
        dfs_2007 = pd.read_excel(xls_file_2007, header = None)
        jan_feb_mar_apr_may_2007.append(dfs_2007)

    df_2007 = pd.concat(jan_feb_mar_apr_may_2007)
    df_2007.drop(columns=[4, 7], inplace = True)

    #Format Columns
    df_2007_formatted = format_columns(df_2007)

    #Add to list of dfs 2001-2007 which will be combined
    dataframes_2001_2007.append(df_2007_formatted)


    ########### 2006 ###########

    
    xls_file_2006 = OUTPUT_DIRECTORY + 'UK_weekend_box_office_reports_2006'

    #Format header ready for concatenation
    dfs_jan_sept_2006 = pd.concat(pd.read_excel(xls_file_2006, sheet_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'], header = None))
    dfs_jan_sept_2006.drop(columns=[4, 6], inplace = True)
    #Format Columns
    dfs_jan_sept_2006_formatted = format_columns(dfs_jan_sept_2006)
    #Add to list of dfs 2001-2007 which will be combined
    dataframes_2001_2007.append(dfs_jan_sept_2006_formatted)

    #Format header ready for concatenation
    dfs_oct_dec_2006 = pd.concat(pd.read_excel(xls_file_2006, sheet_name = ['Oct', 'Nov', 'Dec'], header = None))
    dfs_oct_dec_2006.drop(columns=[4, 7], inplace = True)
    #Format Columns
    dfs_oct_dec_2006_formatted = format_columns(dfs_oct_dec_2006)
    #Add to list of dfs 2001-2007 which will be combined
    dataframes_2001_2007.append(dfs_oct_dec_2006_formatted)
    
       
    ########### 2001-2005 ############


    #create a list of fienames to loop through
    list_of_years_2001_2005 = [str(year) for year in range(2001, 2006)]
    filename_list_2001_2005 = create_filename_list(list_of_years_2001_2005)
    
    for xls_file_2001_2005 in filename_list_2001_2005:

        #concatenate all dataframes from separate sheets
        all_dfs_2001_2005 = pd.read_excel(xls_file_2001_2005, header=None, sheet_name = None)    
        df_2001_2005 = pd.concat(all_dfs_2001_2005)

        ########### DELETE DELETE DELETE DELETE ############
        pd.set_option('display.max_rows', df_2001_2005.shape[0]+1)
        
        #Check for Dataframes with additional 'Site Avg Column'
        if len(df_2001_2005.columns) > 8:
            df_2001_2005.drop(columns=[6], inplace = True)

        #Format header ready for concatenation
        df_2001_2005.drop(columns=[4], inplace = True)
        
        #Format Columns
        df_2001_2005_formatted = format_columns(df_2001_2005)
        #Add to list of dfs 2001-2007 which will be combined
        dataframes_2001_2007.append(df_2001_2005_formatted)
    
    #Combine dataframes to make master DF
    df_2001_2007 = pd.concat(dataframes_2001_2007)

    #Reformat Columns and reset row index
    df_2001_2007.reset_index(drop=True, inplace=True)

    #find index position of date identifiers to remove headers
    df_2001_2007['Film'] = df_2001_2007['Film'].astype(str)
    box_office_filt = df_2001_2007['Film'].str.contains('Weekend Box Office')
    row_indices = df_2001_2007.index[box_office_filt == True].tolist()

    #set up loop for the creation of date of release column
    row_index = 0
    length_of_dataframe = len(df_2001_2007)

    #find the Friday date 
    for date_position in row_indices:
        date_string = df_2001_2007['Film'].iloc[date_position]
        date_string_split = date_string.split()
        if '2007' in date_string_split:
            if len(date_string_split) == 12:
                friday_date = date_string_split[4] + " " + date_string_split[7] + ' 2007'
            else:
                friday_date = date_string_split[4] + " " + date_string_split[5] + ' 2007'
        elif "4-" in date_string_split:
            date = date_string_split[5][0]
            friday_date = date + " " + date_string_split[4] + " "  + date_string_split[8]
        elif len(date_string_split) == 14:
            friday_date = date_string_split[5] + " " + date_string_split[4] + " "  + date_string_split[6] 
        elif len(date_string_split) == 13:
            friday_date = date_string_split[4] + " " + date_string_split[5] + " " + date_string_split[9]
        elif len(date_string_split) == 12:
            if date_string_split[6] == "-":
                friday_date = date_string_split[5] + " " + date_string_split[4] + " " + date_string_split[8]
            else:
                friday_date = date_string_split[6] + " " + date_string_split[5] + " " + date_string_split[9]
        else:
            date = date_string_split[5][0:2]
            friday_date = date + " " + date_string_split[4] + " " + date_string_split[6]

        friday_date_split = friday_date.split()

        try:
            int(friday_date_split[0])
        except:
            friday_date_split[0], friday_date_split[1] = friday_date_split[1], friday_date_split[0]

        if friday_date_split[1] in MONTHS:
            friday_date_split[1] = MONTHS.get(friday_date_split[1])

        friday_date = friday_date_split[0] + " " + friday_date_split[1] + " " + friday_date_split[2]
        release_date_formatted = datetime.strptime(friday_date, '%d %B %Y').date()

        #add the friday date to the columns according to their position on the page
        if row_index +2 <= len(row_indices):
            df_2001_2007.at[row_indices[row_index]:row_indices[row_index+1], 'Date Column'] = release_date_formatted
        else:
            df_2001_2007.at[row_indices[row_index]:length_of_dataframe, 'Date Column'] = release_date_formatted
        
        row_index +=1

    #Drop Excess rows
    df_2001_2007 = df_2001_2007.dropna(subset = ['Rank', 'Total Gross to date'])
    df_2001_2007 = df_2001_2007[df_2001_2007['Film'] != 'Title']
    df_2001_2007 = df_2001_2007[df_2001_2007['Film'] != 'Film']

    LIST_OF_DATAFRAMES.append(df_2001_2007)


    ########### 2007 - 2020 ############


    #Loop through files
    #Get the date of the file
    for xls_file_2007_2020 in filename_list_2007_2020:
        date_of_month_pos = re.search(r"\d", xls_file_2007_2020).start()
        hyphon_pos = xls_file_2007_2020.find('-')
        friday_date = xls_file_2007_2020[date_of_month_pos:hyphon_pos]
        file_name_second_part = xls_file_2007_2020[hyphon_pos:]
        if len(friday_date) <= 2:
            underscore_pos = file_name_second_part.find('_')
            date_and_year = file_name_second_part[underscore_pos:]
            release_date = friday_date + date_and_year
        elif len(friday_date) > 12:
            release_date = friday_date
        else:
            indices = [s.start() for s in re.finditer('_', file_name_second_part)]
            date_and_year = file_name_second_part[indices[1]:]
            release_date = friday_date + date_and_year
        if release_date.endswith('.xls'):
            release_date = release_date.replace('.xls', '')
        release_date_formatted = datetime.strptime(release_date, '%d_%B_%Y').date()

        #Set up dataframes           
        dfs_2007_2020 = pd.read_excel(xls_file_2007_2020, header = None)
    
        if len(dfs_2007_2020.columns) > 10:
            dfs_2007_2020.drop(dfs_2007_2020.iloc[:, 10:], inplace = True, axis = 1)
        
        #Format dataframes
        #Correct column index line
        if 'Rank' in dfs_2007_2020.columns:
                continue
        else:
            #Find the row with the desired header
            index_row_2007_2020 = dfs_2007_2020.index[dfs_2007_2020.iloc[:,0] == 'Rank'].tolist()
            #Move Header row to index
            dfs_2007_2020 = set_column_index(dfs_2007_2020, index_row_2007_2020[0])
        
        #Drop empty and excess column indices
        columns_to_drop = ['Country of Origin', '% change on last week', 'Site average','% chg', 'Site Avg', 'CoO', '%', 'Weekend Loc Avg', 'Running  Total', 'UK=yes', 'BO  of UK films', 'Percentage Change', 'Site Average'] 
        dfs_2007_2020 = drop_excess_columns(dfs_2007_2020, columns_to_drop)

        formatted_2007_2020 = format_columns(dfs_2007_2020)
        formatted_2007_2020['Date Column'] = release_date_formatted
        formatted_2007_2020.dropna(subset = ['Rank', 'Total Gross to date'], inplace=True)

        #Append dataframes to list
        LIST_OF_DATAFRAMES.append(formatted_2007_2020)

    ########### COMBINE, CORRECT COLUMNS, AND WRITE TO FILE ############
    total_combined = pd.concat(LIST_OF_DATAFRAMES)

    #Put Film column in caps and run spell check to eradicate spelling inconsistencies 
    total_combined['Film'] = total_combined['Film'].str.upper()
    total_combined['Film'] = total_combined['Film'].apply(str).apply(spellcheck_film)

    #Set Up opening Weekend Filter
    filt = (total_combined['Weeks on release'] == 1)

    #Add Release Date
    total_combined.loc[filt, 'Release Date'] = total_combined.loc[filt, 'Date Column']
    #Fill empty Release Date rows with the value from week 1
    total_combined['Release Date'] = total_combined.groupby('Film')['Release Date'].bfill()

    #Add Opening Weekend
    total_combined.loc[filt, 'Opening Weekend'] = total_combined.loc[filt, 'Weekend Gross']
    #Fill empty Opening Weekend rows with the value from week 1
    total_combined['Opening Weekend'] = total_combined.groupby('Film')['Opening Weekend'].bfill()

    #Add Screen Average
    #Format Columns for ready for division
    total_combined['Weekend Gross'] = total_combined['Weekend Gross'].apply(apply_float)
    total_combined['Number of cinemas'] = total_combined['Number of cinemas'].apply(apply_float)
    #Divde columns for Screen Average
    total_combined['Screen Average'] = total_combined['Weekend Gross'] / total_combined['Number of cinemas']
    #Fill screen average with mean average from run
    total_combined['Screen Average'] = total_combined.groupby('Film')['Screen Average'].transform('mean')

    #Get values for highest rank column by sorting films by Rank and dropping duplicates
    total_combined['Rank'] = total_combined['Rank'].apply(apply_float)
    df_highest_rank = total_combined.sort_values('Rank').drop_duplicates('Film', keep='first')
    df_highest_rank.set_index('Film', inplace = True)
    df_highest_rank = df_highest_rank['Rank']
    
    #Get values for length of run column by sorting films by Weeks on Release and dropping duplicates
    total_combined['Weeks on release'] = total_combined['Weeks on release'].apply(apply_float)
    df_length_of_run = total_combined.sort_values('Weeks on release').drop_duplicates('Film', keep='last')
    df_length_of_run.set_index('Film', inplace = True)
    df_length_of_run = df_length_of_run['Weeks on release']
    #Drop the columns that we no longer need
    total_combined = total_combined.drop(columns = ['Rank', 'Weekend Gross', 'Weeks on release', 'Number of cinemas', 'Date Column'])
    
    #Drop dupicated columns from original dataframe, keeping Total Gross to date
    total_combined['Total Gross to date'] = total_combined['Total Gross to date'].astype(str).str.replace(',', '').astype(float)
    total_sorted = total_combined.sort_values('Total Gross to date').drop_duplicates('Film', keep='last')
    
    total_sorted.set_index('Film', inplace = True)

    df_merged = pd.merge(left=total_sorted, right=df_highest_rank, how='left', left_on='Film', right_on='Film')
    df_merged = pd.merge(left=df_merged, right=df_length_of_run, how='left', left_on='Film', right_on='Film')

    df_merged = df_merged.rename(columns={'Rank': 'Highest Rank', 'Weeks on release': 'Length of Run'})

    df_merged.to_csv(OUTPUT_DIRECTORY + '/UK_Box_Office_Compiled_Master')

    print('Done')