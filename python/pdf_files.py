from datetime import datetime
import pandas as pd
import subprocess
import tabula

from insiders_pdf.pdf_files_data import get_value_lst, price_volume
from insiders_pdf.pdf_files_subcat import subcategories
from insiders_pdf.pdf_files_errors import Error_Columns_Mismatch
from mysql_db import table_management
from config import config

pd.set_option('display.max_columns', None)


'''
Założenia:
1.  Format transakcji na akcjach jest w miarę ustandaryzowany pod względem następujących po sobie części
    lub ilości kolumn i nazw poszczególnych informacji

Schemat działania:
1.  Algorytm bierze nowe raporty z tabeli data_files i sprawdza ich kategorie a następnie analizuje pdf
2.  Do osobnej tabeli wrzucane są pdfy lub jpg z prawdopodobnie zdjęciami zamiast tabelami


Jedyny błąd - rzadki:
1. jeśli inf zbiorcze są na tab nr 2 i wyskoczy błąd - tzn nan lub 0 to sprawdzamy tą tabelę - a lista z transakcjami 
jest na poprzedniej tabeli (dużo kodu aby uniknąć tego błędu który pojawia się rzadko) 


koszt pozyskania tabeli z skanowanego pdf to 10 gr za stronę / pdf?

Do zmiany:
1. wprowadzić batchsize po jakim będziemy sprawdzać transkacje? #zrobione 
2. relatywne pobieranie danych z bazy - pdfy #zrobione 
3. aktualizowanie config co batchsize #zrobione 
4. sprawdzenie co w przypadku próby zapisania tej samej transakcji pdf - czy jest hash lub po report_id #zrobione 
'''

#zmienne do logowania do bazy
hostname = config['hostname']
dbname = config['dbname']
uname = config['uname']
pwd = config['pwd']


table_config_table = config['table_config_table']
table_aalerts_backend = config['table_aalerts_backend']

table_data = config['table_data']
table_data_file = config['table_data_file']

table_transactions_pdf_files = config['table_transactions_pdf_files']
table_transactions_pdf_rejected = config['table_transactions_pdf_rejected']
table_transactions_pdf_scanned = config['table_transactions_pdf_scanned']
table_fma_cms_alert_categories = config['table_fma_cms_alert_categories']

#zmienne
cat_id = int(config['pdfs_cat_id'])
pdf_limit_batchsize = int(config['pdf_limit_batchsize'])


multi_term_lst_dict = {'podmiot': ['nazwa/nazwisko', 'imięinazwisko', 'imieinazwisko', 'nazwa'],
                       'stanowisko': ['stanowisko/status', 'więziprawnejłączącejosobęzob'],
                       'cena i wol': ['informacjezbiorcze', 'łącznywolumen'],
                       'opis instrumentu': ['opisinstrumentufinansowego', 'wskazanieinstrumentufinan'],
                       'rodzaj': ['rodzajtransakcji'],
                       'data': ['datatransakcji', 'dataigodzina', 'dataimiejscezaw'],
                       'miejsce': ['miejscetransakcji', 'miejscezawarciatransa']
                       }

multi_term_lst = list(multi_term_lst_dict.values())
multi_term_lst_val = list(multi_term_lst_dict.keys())
date = datetime.now()


def alerts_table(info):
    """ system alertowania w przypadku awarii pobierania szczegółowych danych o jakiejś spółce """
    cls = table_management(hostname, dbname, uname, pwd)
    cls.add_data_row(table_aalerts_backend, [info, date, 'pdf_files_insiders'], '(info,updated,table_name)', '(%s, %s, %s)')
    cls.close_connection_2()


def get_ids_insider():
    """ pobieranie id raportów już obecnych w tabelach insiderów, większych od branego pod uwagę ID """
    cls = table_management(hostname, dbname, uname, pwd)
    id_to_start = cls.fetch_all_results_filtered(table_config_table, 'config', f'id = 3')  # wszystkie raporty z kategorią 16 - transakcje na akcjach
    id_to_start = id_to_start[0][0]

    pdf_files_ids = cls.fetch_all_results_filtered(table_transactions_pdf_files, 'report_id', f'report_id > "{id_to_start}"')
    pdf_files_ids_rejected = cls.fetch_all_results_filtered(table_transactions_pdf_rejected, 'report_id', f'report_id > "{id_to_start}"')
    pdf_files_ids_scanned = cls.fetch_all_results_filtered(table_transactions_pdf_scanned, 'report_id', f'report_id > "{id_to_start}"')
    cls.close_connection_2()

    pdf_files_all_ids = pdf_files_ids + pdf_files_ids_scanned + pdf_files_ids_rejected
    pdf_files_all_ids = [x[0] for x in pdf_files_all_ids]
    return list(set(pdf_files_all_ids))


class report_data:

    all_ids_insiders = get_ids_insider()  #id raportów w tabelach insiderów z id większym od x...

    def last_id_tabs(self):
        """ id pobieramy większe od ostatniego sprawdzonego - a potem najwyżej jeszcze dodatkowo sprawdzamy w tabeli z insiderami """
        cls = table_management(hostname, dbname, uname, pwd)
        id_to_start = cls.fetch_all_results_filtered(table_config_table, 'config', f'id = 3')  # wszystkie raporty z kategorią 16 - transakcje na akcjach
        id_to_start = id_to_start[0][0]
        print(f"Id od którego zaczynamy szukać komunikatów z transakcjami insiderów: {id_to_start}")

        table_1 = table_fma_cms_alert_categories
        table_2 = table_data
        table_3 = table_data_file
        where_condition = f'''{table_1}.alert_id = {table_2}.id AND {table_2}.id = {table_3}.idData AND 
                              {table_1}.category_id = {cat_id} AND {table_2}.id > {id_to_start} LIMIT {pdf_limit_batchsize}'''
        cols_1 = []
        cols_2 = ['id', 'company_id', 'time']
        cols_3 = ['source']
        df = cls.fetch_data_three_tables(table_1, table_2, table_3, cols_1, cols_2, cols_3, where_condition)

        cls.close_connection_2()
        df = df[~df['id'].isin(self.all_ids_insiders)]
        return df

    def get_pdfs(self):
        """
        Funkcja do pobierania danych z pdfów. Jeśli wystąpi błąd pobierania to:
            -   jeśli nie zostaną wykryte tabele to zakładamy że prawdopodobnie jest to pdf ze skanem tabeli
                (do sprawdzenia z rozwiązaniem AI do sczytywania tabel)
            -   w przeciwnym razie po prostu zapisujemy to jako błędny pdf
        """
        df = self.last_id_tabs()
        print(f"Do sprawdzenia jest {len(df)} raportów")

        if df.empty is False:
            for row in df.to_dict('records'):
                report_id, link, comp_id, pub_date = row['id'], row['source'], row['company_id'], row['time']
                link = 'http://biznes.pap.pl/pl/news/espi/attachment/35768386,200402_JG_-_oswiad._nabycie_akcji.pdf,20200402/'
                #link = 'http://biznes.pap.pl/pl/news/espi/attachment/30630921,180613_OPF_-_MAR.pdf,20180614/'
                try:
                    tables = tabula.read_pdf(link, multiple_tables=True, pages='all', options="--pages 'all'", lattice=True)

                    if len(tables) == 0:
                        print(f"Skan: {link}")
                        #self.save_pdfs_with_error(table_transactions_pdf_scanned, link, report_id, comp_id, date)
                    else:
                        cls = try_get_pdf_data(tables, report_id, link, comp_id, pub_date, self.all_ids_insiders)
                        cls.process_tables()

                except (ValueError, FileNotFoundError):
                    print(f"Odrzucony: {link}")
                    #self.save_pdfs_with_error(table_transactions_pdf_rejected, link, report_id, comp_id, date)
                    ''' błąd przy pobieraniu - rejected '''
                except Error_Columns_Mismatch as e:
                    info = f"Długości list do złącznia w df nie są sobie równe (nazwa, instrument_finnsowy, cena ...), link: {link}, report_id: {report_id}, error: {e}"
                    print(info)
                    #alerts_table(info)
                    #self.save_pdfs_with_error(table_transactions_pdf_rejected, link, report_id, comp_id, date)
                except subprocess.CalledProcessError as e:
                    if '.jpg' in link:  #prawdopodobnie dlatego
                        print(f"Skan: {link}")
                        #self.save_pdfs_with_error(table_transactions_pdf_scanned, link, report_id, comp_id, date)
                    else:
                        info = f"Zapisano niezidentyfikowany plik pdf do tabeli odrzuconych pdf-ów. Błąd: subprocess.CalledProcessError - pojawiający się przy plikach .jpg, Error: {e}, link: {link}"
                        alerts_table(info)
                        print(f"Odrzucony: {link}")
                        #self.save_pdfs_with_error(table_transactions_pdf_rejected, link, report_id, comp_id, date)
                break


    def save_pdfs_with_error(self, table_name, link, report_id, comp_id, date):
        dict_data = {
            'pdf_link': link,
            'report_id': report_id,
            'comp_id': comp_id
        }
        cls = table_management(hostname, dbname, uname, pwd)

        col_names = list(dict_data.keys())
        col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
        values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"
        data = list(dict_data.values())

        if report_id not in self.all_ids_insiders:  #jeśli nie ma jeszzcze tego id reaport w tabelach insiderów
            cls.add_data_row(table_name, data, col_names_string, values_string)

        cls.update_value(table_config_table, 'config', report_id, 'id', '3')
        cls.update_value(table_config_table, 'updated_at', f'{date}', 'id', '3')
        cls.close_connection_2()




''' wyciąganie konkretnych danych z pdfów '''

def clean_table(table):
    """ set header as first row """
    table.loc[-1] = table.columns  # adding a row
    table.index = table.index + 1  # shifting index
    table = table.sort_index()  # sorting by index

    ''' change columns names - to digits '''
    lst_names = list(range(0, len(table.columns)))
    table.columns = lst_names

    ''' set nan or empty value as missing '''
    table = table.applymap(lambda s: s.lower() if type(s) == str else s)  # małe litery jeśli są słowa
    table = table.fillna('missing')
    table = table.replace('', 'missing')
    return table


def get_cleaned(tables):
    print("to jest ilość wykrytych tabel: %s " % len(tables))
    cleaned_tables = []

    for table in tables:
        table_cleaned = clean_table(table)
        cleaned_tables.append(table_cleaned)
    return cleaned_tables

def get_transaction_value(row):
    """ obliczanie wartości transakcji """
    price = round(float(row['price']), 4)
    volume = round(float(row['volume']), 2)
    return round(price*volume, 2)

def find_header_row(df):
    """ funkcja do sprawdzania czy w df jest wiersz z header i jeśli tak to go odrzucamy """
    index = 0
    for row in df.to_dict('records'):
        row_string = ' '.join([str(x) for x in row.values()])
        if 'powiadomienie o transakcji/transakcjach' in row_string:
            return index
        index += 1
    return False


class try_get_pdf_data:

    def __init__(self, tables, report_id, link, comp_id, pub_date, pdf_files_ids):
        self.tables = tables
        self.report_id = report_id
        self.link = link
        self.comp_id = comp_id
        self.pub_date = pub_date
        self.pdf_files_ids = pdf_files_ids

    def process_tables(self):
        cleaned_tables = get_cleaned(self.tables)  # lista z oczyszczonymi tabelami z pdfów
        df_pdf = self.get_multi_data(cleaned_tables)

        cls3 = subcategories(df_pdf)
        df = cls3.determine_subcat()
        print(f"Wykryto poprawie transakcję insidera, link: {self.link}")
        df['pdf_link'], df['pubDate'], df['comp_id'], df['report_id'] = self.link, self.pub_date, self.comp_id, self.report_id
        self.update_pdf_table(df)

    def get_multi_data(self, cleaned_tables):
        instr_fin = []
        rodz_trans = []
        data_trans = []
        msc_trans = []
        price_vol = []
        nazw = []
        status = []

        #cleaned_tables = cleaned_tables[:1]

        for table in cleaned_tables:
            index_value = find_header_row(df=table)
            if index_value is not False:
                table = table.drop([table.index[index_value]])
            table_cleaned = table.replace(to_replace=r'\s+', value=' ', regex=True).replace(to_replace=r'\r+', value=' ', regex=True).reset_index(drop=True)
            print(table_cleaned)

            for column_name, term_lst in multi_term_lst_dict.items():
                regex_term = '|'.join(term_lst)
                cls = get_value_lst()
                value_lst = cls.get_values(regex_term, table_cleaned)

                if term_lst == multi_term_lst[2]:  #musi być tutaj bo wtedy nie jest powtarzana pętla 'for lst in search_lst' jeśli jest wiecej niż jeden - a podajemy i tak tabelę (price_volume) - więc wyniki się dublikują
                    cls = price_volume(regex_term, table)  #podajemy czystą i nieprzetworzoną tabelę
                    price_and_vol = cls.search_raw_table()
                    if len(price_and_vol) != 0:
                        for elem in price_and_vol:
                            price_vol.append(elem)

                if column_name in ['podmiot', 'stanowisko']:  #jeśli są to dane pojedyńcze to zawsze bierzemy pierwszą wartość z listy
                    value_lst = value_lst[:1] if len(value_lst) > 0 else value_lst

                for value in value_lst:
                    if term_lst == multi_term_lst[3]:
                        instr_fin.append(value)
                    elif term_lst == multi_term_lst[4]:
                        rodz_trans.append(value)
                    elif term_lst == multi_term_lst[5]:
                        data_trans.append(value)
                    elif term_lst == multi_term_lst[6]:
                        msc_trans.append(value)
                    elif term_lst == multi_term_lst[0]:
                        nazw = value
                    elif term_lst == multi_term_lst[1]:
                        status = value

        if len(price_vol) < len(rodz_trans):  #jeśli brakuje danych ocenie
            price_vol = tuple([(0, 0)]*len(rodz_trans))

        print(instr_fin)
        print(price_vol)
        print(data_trans)
        print(msc_trans)
        print(rodz_trans)
        print(nazw)
        print(status)

        if len(instr_fin) == len(price_vol) == len(rodz_trans) == len(msc_trans) == len(data_trans):
            df = pd.DataFrame(
                {multi_term_lst_val[0]: nazw, multi_term_lst_val[1]: status, multi_term_lst_val[2]: price_vol,
                 multi_term_lst_val[3]: instr_fin, multi_term_lst_val[4]: rodz_trans,
                 multi_term_lst_val[5]: data_trans, multi_term_lst_val[6]: msc_trans})
            df[['price', 'volume']] = pd.DataFrame(df[multi_term_lst_val[2]].tolist(), index=df.index)
            df['value'] = df.apply(lambda row: get_transaction_value(row), axis=1)
            df = df.drop([multi_term_lst_val[2]], axis=1)
            return df
        else:
            raise Error_Columns_Mismatch

    def update_pdf_table(self, df):
        """
        zapisywanie do bazy danych - jednak jeśli jest już taki hash w bazie to wtedy pomijamy pdf i dodajemy info z tabeli z alertami
        jeśli pdf zostanie zapisany do bazy poprawnie to zmieniamy ostatni id raportu w tabeli config >> zmiany następują po każdej iteracji
        Updatujemy po try except bo, jeśli transakcja znajduje się już w tabeli to powstanie błędne koło (bo kolejna iteracja się zacznie od tego samego id
        :param df: transakcje z jednego pdfu do zapisania w bazie
        """
        cls = table_management(hostname, dbname, uname, pwd)
        print(df)

        if self.report_id not in self.pdf_files_ids:
            cls.insert_df(df, table_transactions_pdf_files, 'append', False)  ##how: 'append' or 'replace' or ... working ex: (df, 'gpw_companies', 'replace', False)

        cls.update_value(table_config_table, 'config', self.report_id, 'id', '3')
        cls.update_value(table_config_table, 'updated_at', f'{date}', 'id', '3')
        cls.close_connection_2()


"""
raise CalledProcessError(retcode, process.args,
subprocess.CalledProcessError: Command '['java', '-Dfile.encoding=UTF8', '-jar', 'C:\\Users\\m_met\\anaconda3\\envs\\macro_data\\lib\\site-packages\\tabula\\tabula-1.0.5-jar-with-dependencies.jar', '--pages', 'all', '--pages', 'all', '--lattice', '--guess', '--format', 'JSON', 'C:\\Users\\m_met\\AppData\\Local\\Temp\\9cda0d47-535d-4133-99c1-a2d9118f569f.pdf']' returned non-zero exit status 1.
"""
