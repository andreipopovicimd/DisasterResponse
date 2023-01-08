import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):

    #Read CSV to DFs
    df_messages = pd.read_csv(messages_filepath)
    df_categories = pd.read_csv(categories_filepath)

    #Join DFs on 'id'. Messages with no categories data will be dropped
    df = df_messages.merge(df_categories, on='id', how='inner')
    
    return df


def clean_data(df):
    
    #Create a DF that contains 1 category in each column
    df_categories_split = df.categories.str.split(pat=';', expand=True)
    
    #Extract column names from the first row
    column_names = df_categories_split.iloc[0].apply(lambda x: x[:-2])
    df_categories_split.columns = column_names

    #Keep only last character in the data and convert it to number
    for column in df_categories_split.columns:
        df_categories_split[column] = df_categories_split[column].apply(lambda x: x[-1])
        df_categories_split[column] = pd.to_numeric(df_categories_split[column])
    
    #Horizontally append the DF with categories data to the original DF
    df = pd.concat([df, df_categories_split], axis=1)
    
    df = df.drop('categories', axis=1)
    df = df.drop_duplicates()
        
    return df


def save_data(df, database_filename):
    engine = create_engine('sqlite:///DisasterResponse.db')
    df.to_sql('LabeledMessages', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()