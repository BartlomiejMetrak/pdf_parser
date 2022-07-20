from dateparser.search import search_dates
import dateutil.parser as dparser
from datetime import datetime
import pandas as pd
import ast
import re

from mysql_db import table_management
from config import config

'''
Algorytm do nadawania subkategorii poszczególnym elementom w plikach pdf

do zrobienia:
> stopniowanie co do czego przydzielać - np akcje w ramach programu motywacyjnego i zwykłe akcje (dodać rank?) - dodać ranking i przemyśleć
czy nie trzeba zamienić słów na liczby w tabeli w bd
'''

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']

#table_transactions_share_subcat
table_transactions_share_subcat = config['table_transactions_share_subcat']

#zmienne
subcat_dict = ast.literal_eval(config['pdfs_subcat_dict'])
polish_chars = ast.literal_eval(config['polish_chars'])


'''
nazwy kolumn w tabeli z transakcjami - jeśli jakaś kolumna zostanie zmieniona to tutaj też automatycznie - jednak 
categoryID w tabeli transactions_share_cat pozostanie niezmienne - ale nie będzie to błąd bo dalej ta kategoria
będzie miała ten sam unikalny ID i wskazywała na subkategorie dla dobrej kategorii
'''

months = {'january': ['styczeń', 'stycznia', 'sty'],
          'february': ['luty', 'lutego', 'lut'],
          'march': ['marzec', 'marca', 'mar'],
          'april': ['kwiecień', 'kwietnia', 'kwi'],
          'may': ['maj', 'maja', 'maj'],
          'june': ['czerwiec', 'czerwca', 'cze'],
          'july': ['lipiec', 'lipca', 'lip'],
          'august': ['sierpień', 'sierpnia', 'sie'],
          'september': ['wrzesień', 'września', 'wrz'],
          'october': ['październik', 'października', 'paź', 'paz'],
          'november': ['listopad', 'listopada', 'lis'],
          'december': ['grudzień', 'grudnia', 'gru'],
          }

months_variants = []
for month in months.values():
    months_variants = months_variants + month



class subcategories:

    def __init__(self, df_pdf):
        self.df_pdf = df_pdf

        cls = table_management(hostname, dbname, uname, pwd)
        self.df_subcat = cls.get_columns_data(table_transactions_share_subcat, '*')
        cls.close_connection_2()

    def determine_subcat(self):
        df = self.df_pdf.copy()
        df = subcategories.determine_post(self, df)
        df = subcategories.determine_date(self, df)
        df = subcategories.determine_data(self, df)  #musi być na końcu bo zmieniane są wartości - wcześniejsze funkcje są bardziej precyzyjne
        return df

    def determine_date(self, df):  #ta funkcja przelicza dosyć długo więc w przyszłości może przemyśleć jej przeprogramowanie
        """ funkcja wyszukuje w zdaniu datę, jeśli jest kilka dat do bierzemy tą pierwszą """
        date_lst = df['data'].tolist()
        dateime_format_lst = []

        for raw_string_date in date_lst:  #dla każdego wiersza w tabeli - gdzie jest data
            date_parsed = self.clean_date_string(raw_string_date)
            dateime_format_lst.append(date_parsed)  # bierzemy tylko pierwszą datę jeśli jest wiecej na liście, a jak nie ma to data jest równa pd.NA
        df['data'] = dateime_format_lst #dateime_format_lst
        return df

    def clean_date_string(self, date_string):
        if any(" " + month_variant + " " in date_string for month_variant in months_variants):
            for month, variants in months.items():
                if any(" " + variant + " " in date_string for variant in variants):
                    pattern = ' ' + ' | '.join(variants) + ' '
                    to_replace = " " + month + " "
                    date_string = re.sub(pattern, to_replace, date_string)
        try:
            dates = dparser.parse(date_string, fuzzy=True, default=None)  #działa tylko w przy[adku jednej daty
        except ValueError:
            dates = self.search_dates_attempt_2(date_string)  #jeśli więcej dat to sprawdzamy po tej funkcji
        return dates

    @staticmethod
    def search_dates_attempt_2(raw_string_date):
        """ sprawdzanie wielu dat - może być tak że będzie błąd i zapisze się data dzisiejsza!! - uważać w przyszłości """
        dates = search_dates(raw_string_date)
        if dates is None:
            return None

        date_string = pd.NA  # jeśli nie zostanie znaleziona data to będzie pd.NA
        for x in dates:  # tyle ile zostało wyszukanych dat w komórce tabeli
            for i in range(len(x)):  # dla każdego formatu jaki został znaleziony - z jednej daty moze być string i datetime
                if isinstance(x[i], datetime):
                    date_string = x[i]
                    break
            else:  # żeby wyjść z nested loops przy break
                continue  # only executed if the inner loop did NOT break
            break  # only executed if the inner loop DID break
        return date_string

    def determine_post(self, df):
        """ zakładamy że własciwe słowo kluczowe to, te które zawiera któreś z listy char_clue """

        df = df.replace(to_replace=r'\s+', value=' ', regex=True)  # zamiana wszystkich white spaces na spacje
        stanowisko_lst = df['stanowisko'].tolist()
        char_lst = ['/']  #znaki kluczowe wskazujące że jest kilka wymienianych stanowisk itp
        #char_clue = ['*']  #znaki wskazujące, że może to być to dane stanowisko
        final_post_list = []

        for value in stanowisko_lst:  #dla danego wiersza
            if any(x in value for x in char_lst):  #jeśli jest jakikolwiek przerywnik w ciągu znaków typu (prezes / dyrektor ...)
                final_post_list.append('inne')
            else:
                final_post_list.append(value)
        df['stanowisko'] = final_post_list
        return df

    def determine_data(self, df_pdf_updated):
        df_pdf_temp = df_pdf_updated.copy()
        df_pdf_temp = df_pdf_temp.replace(polish_chars, regex=True)  #zamiana z polskich znaków typpu ą na a itd..
        df_pdf_temp = df_pdf_temp.replace(to_replace=r'\s+', value=' ', regex=True)  #zamiana wszystkich white spaces na spacje


        for categoryID, column_pdf in subcat_dict.items():  #dictionary - ustalony na stałe - nazwa kolumny i jej ID
            df_instrument = self.df_subcat[self.df_subcat['categoryID'] == categoryID].sort_values(by='subcategoryID', ascending=True)  # sortowanie żeby wyszukiwać najpierw akcje - program motywacyjny a potem akcje
            df_pdf_updated[column_pdf] = 'inne'
            for i, row in df_instrument.iterrows():
                string = row['string']
                subcat = row['subcategory']
                subcatID = row['subcategoryID']

                valueString = ''

                elementList = string.split(';')
                for eachValue in elementList:
                    valueString += f'(?=.*{eachValue})'

                df_pdf_updated.loc[df_pdf_temp[column_pdf].str.contains(valueString), column_pdf] = subcat  #w oficjalnym df zapisujemy słowa i nieoczyszczone
                df_pdf_temp.loc[df_pdf_temp[column_pdf].str.contains(valueString), column_pdf] = str(subcatID)  #temp_df używamy do oznaczania już znalezionych wartości
        return df_pdf_updated
