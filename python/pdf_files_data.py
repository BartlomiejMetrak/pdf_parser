import pandas as pd
import copy
import math
import ast
import re

from config import config

pd.set_option('display.max_columns', None)

'''
założenia price_and_volume:
1. jeśli sa trzy wyrażenia w kamórce i jedno z nich do PLN to podstawowy podział
2. jeśli są trzy wyrażenia w komórce i nie jest to zał. 1. to zakłądamy że są to trzy float i próbujemy je zaminić ze string na float
jeśli się uda to sprawdzamy czy jest to wynik wolumen*price=total_value - jeśli się sprawdzi to zakładamy że na 1 miejscu jest wolumen i na 2 jest price
3. jeśli żaden z powyższych to oznaczany price i wolumen jako pd.NA

5. jeśli wyszukujemy po transakcjach, a nie infor. zbiorczych (ostatnia próba) to bierzemy pod uwagę tylko te które są wczesniej równe 0.0
Wyszukujemy tylko w tabelach w których mamy inform zbiorcze oraz cena i wolument - wiersze w jednej tabeli i zakładamy że kolumna cena i kolumna wolumen są nad 
wartościami odpowiadającymi poszczególnym transakcjom (nie ma przesunięcia w tabeli)

do rozważenia:
wyszukiwanie wariancji rubryk do tabeli typu - nazwa/nazwisko lub imie i nazwisko
przykł: https://biznes.pap.pl/pl/news/espi/attachment/40067822,Standard_form_-_Notification_of_transactions_art._19_MAR_Pawel_Przewiezlikowski.pdf,20210813/
(tutaj też prezes zarządu / president) >> dlatego że / to zamiana na inny (pomyśleć jak to rozwiązać)

price i volume dla (4 przypadki - pcf group):  https://biznes.pap.pl/pl/news/espi/attachment/40200532,Powiadomienie_Notification_art._19_MAR_20210830.pdf,20210830/
'''

currency_lst = ast.literal_eval(config['pdfs_currency_lst'])


class get_value_lst:

    def get_values(self, search_term, table):
        res_rows = get_value_lst.search_for_term(self, search_term, table)  # wyszukiwanie stringu w tabeli i zwracanie całego rzędu
        res_rows_lst = res_rows.values.tolist()
        value_lst = []
        for lst in res_rows_lst:
            value = get_value_lst.get_value(self, lst, search_term)  # wyszukiwanie wartości w pojedyńczym wierszu
            if value != 'fpjnweiupgn':
                value_lst.append(value)
        return value_lst

    def search_for_term(self, term: str, df, case=False):
        """
        Wyszukiwanie stringu w dataframie - cały dataframe nie jest zmieniany, ale przy wyszukiwaniu usuwane są spacje
        :param term: keyword do wyszukania
        :param df: tabela w pdfu
        :param case: czy case sensitive
        :return: zwracane są wiersze, które maja w sobie szukany string
        """
        textlikes = df.select_dtypes(include=[object, "string"])
        df_rows = df[textlikes.apply(lambda column: column.replace(to_replace=r'\s+', value='',
                                                                   regex=True).str.contains(term, regex=True, case=case,
                                                                                            na=False)).any(axis=1)]
        return df_rows

    def get_value(self, lst, search_term):
        """
        place - where the final value is placed in refer to search term
        podczas wyszukiwania nie brane są pod uwagę spacje - jednak końcowy string jest niezmieniony
        :param lst: lista - cały wiersz dataframu
        :param search_term: szukana kategoria
        :return: zwracany jest pierwszy wyraz, który nie jest missing i wystąpi po szukanej kategorii
        """
        flag = 0
        value = 'fpjnweiupgn'  # specjalnie taki znak - jeśli potem nie zostanie zmieniona to jej nie dodajemy do listy

        for elem in lst:
            print(elem)
            elem = str(elem)
            if flag == 1 and elem != 'missing':  # pierwsza wartość od momentu słowa kluczowego która nie jest 'missing'
                value = elem
                break
            search = re.search(search_term, elem.replace(' ', ''))
            if search:
                flag = 1
        return value  # zwracana wartość, nie lista, bo i informacje zbiorcze są ok


'''
wyciąganie średniej ceny i wolumenu
Ewentualnie w przyszłości mozemy jeszcze poprawic wyciąganie danych z listy transakcji - a nie informacji zbiorczych
teraz szukanie po liście jest wtedy gdy informacje zbiorcze są równe 0 i nie pd.NA
można pomyśleć żeby zamiast domyślnie dawać pd.NA transakcjom zbiorczym dawać na początku 0 a jeśli w liście transakcji nie zostanie odnaleziona wartośc to wtedy pd.NA
'''


def is_int(value):
    """ podajemy string """
    try:
        int(str(value))
        return True
    except:
        return False

def final_volume_check(volume):
    """ ostateczne sprawdzanie wolumenu - po kropkach - czy zamieniać np 21.543 na 21543 lub 23.0 to 23 (nie 230)"""
    volume_string = str(volume)
    dot_counter = volume_string.count('.')
    if dot_counter == 0:  #jeśli są kropki to sprawdzamy miejsca po przecinku
        return int(volume)
    elif dot_counter == 1:  #jeśli jeden to może być albo tys albo float
        dot_index = volume_string.index('.')
        if dot_index == len(volume_string)-4:  #jeśli miejsce kropki jest na 4 miejscu od końca
            volume_string = volume_string.replace('.', '')
            return int(volume_string)
        else:
            return int(float(volume_string))
    else:
        volume_string = volume_string.replace('.', '')
        return int(volume_string)



class price_volume:

    def __init__(self, search_term, table):
        self.search_term = search_term
        self.table = table

    def search_raw_table(self):
        """zakładamy, że szukana wartość jest o jedną dalej od słowa kluczowego np informacje zbiorcze"""

        cls = get_value_lst()
        value_lst = cls.get_values(self.search_term, self.table)
        share_info_lst = []
        dig_variable = 0  # inaczej bralibyśmy tylko pierwszą listę za każdem razem jak w jednej tabeli poajwia się więcej list z transakcjami

        for value in value_lst:
            share_price, volume = price_volume.get_price_and_vol(self, value)

            if (pd.isna(share_price) is True and pd.isna(volume) is True) or (share_price == 0 and volume == 0):
                try:
                    share_price, volume = price_volume.if_both_zeros(self, dig_variable)
                except:
                    share_price = 0.0
                    volume = 0.0

            if pd.isna(volume) is False:
                volume = final_volume_check(volume=volume)  #jeśli wolumen został wykryty (nie pd.NA) to zakładamy że nie może być float

            dig_variable += 1  # dla kolejnych znalezionych list w tej samej tabeli

            if pd.isna(share_price) is False or share_price is not None:  # bierzemy wartości bezwględne
                share_price = abs(float(share_price))
            if pd.isna(volume) is False or volume is not None:  # bierzemy wartości bezwględne
                volume = abs(float(volume))

            var = (share_price, volume)
            share_info_lst.append(var)
        return share_info_lst  # zwracana jest lista z danymi o cenie i wolumenie

    def get_price_and_vol(self, value_string_raw):
        """
        zakładamy, że cena i wolumen tworzą tylko 3 elementy w liście - obok pln
        zakładamy że oznaczeniem waluty jest jedna z currency list - przynajmniej musi to zawierać - czyli np też pln. etc
        """
        share_price = pd.NA
        volume = pd.NA

        value_string = re.sub(r'\s+', ' ', value_string_raw).replace(',', '.')  # zamiana white spaces na pojedyncze i , na . - żeby było widać foaty

        PATTERN = r'[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
        rx = re.compile(PATTERN, re.VERBOSE)
        digits = re.findall(rx, value_string)  # wyszukiwanie wszystkich liczb w wyrażeniu

        if len(digits) == 2:  # jeśli są dwa float to sprawdzamy czy jest jakieś z słowem z currency_lst i oczyszczamy całe wyrażenie
            share_price, volume = price_volume.edit_string_extract_digits(self, value_string, digits, currency_lst)
        elif len(digits) == 3:  # jeśli nie jest to powyższe to zakłądamy ze są to 3 float lub 2 liczby z czego jedna typu  10 000 zamiast 10000
            try:
                value_lst = [float(x) for x in digits]  # zakładamy że to są 3 float - price, wolumen i suma
                first = value_lst[0]
                second = value_lst[1]
                trd = value_lst[2]
                if bool(math.isclose(first * second, trd, rel_tol=0.05)) is True:  # jeśli wynik mnożenia się sprawdzi (+- 5%) to oznaczamy price i volume
                    volume = first  # czyste założenie że wolumen jest na pierwszym miejscu
                    share_price = second
            except:
                share_price = pd.NA
                volume = pd.NA
            else:
                string_lst = value_string_raw.split('\r')
                string_lst = [x for x in string_lst if x != '']
                if len(string_lst) == 2:
                    string_lst = [x.replace(' ', '') for x in string_lst]
                    string_lst = ' '.join(string_lst)
                    value_string = re.sub(r'\s+', ' ', string_lst).replace(',', '.')  # zamiana white spaces na pojedyncze i , na . - żeby było widać foaty
                    PATTERN = r'[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
                    rx = re.compile(PATTERN, re.VERBOSE)
                    digits = re.findall(rx, value_string)  # wyszukiwanie wszystkich liczb w wyrażeniu
                    if len(digits) == 2:
                        share_price, volume = price_volume.edit_string_extract_digits(self, value_string, digits, currency_lst)
        elif len(digits) == 4:  # jeśli są 4 to zakładamy że jest np 20 000 akcji po 10 000 zł zamiast 20000 i 10000
            string_lst = value_string_raw.split('\r')
            if len(string_lst) == 2:
                string_lst = [x.replace(' ', '') for x in string_lst]
                string_lst = ' '.join(string_lst)
                value_string = re.sub(r'\s+', ' ', string_lst).replace(',', '.')  # zamiana white spaces na pojedyncze i , na . - żeby było widać foaty
                PATTERN = r'[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
                rx = re.compile(PATTERN, re.VERBOSE)
                digits = re.findall(rx, value_string)  # wyszukiwanie wszystkich liczb w wyrażeniu
                if len(digits) == 2:
                    share_price, volume = price_volume.edit_string_extract_digits(self, value_string, digits, currency_lst)
        return share_price, volume

    def edit_string_extract_digits(self, value_string_raw, digits, currency_lst):
        """
        edytowanie całego wyrażenia do postaci liczba, liczba i PLN etc (jesli są wykryte dwie liczby)
        :param value_string_raw: string z tabeli - ze spacjali itp - nieoczyszczony
        :param digits: wszystkie liczby w wyrażeniu
        :param currency_lst: lista walut - zmienna w .env
        :return:
        """
        value_string = copy.copy(value_string_raw)
        int_eval = [is_int(x) for x in digits]  # w liście digits są tylko liczby

        for digit in digits:  # jeśli jest zero to kicha
            pattern = ' ' + digit + ' '  # oddzielamy floaty spacjami
            value_string = re.sub(f'(?<![0-9,.]){digit}(?![0-9,.])', pattern, value_string)  # wyszukujemy liczbę nie otoczoną innymi liczbami lub , lub . (eliminujemy 0 w 500 == 5 0 0)
        if any(substring in value_string for substring in currency_lst):  # jeśli w wyrażeniu jest jakiś znak typu zł / pln / zl etc..
            value_string = value_string.split(' ')
            res_lst = []

            for elem in value_string:
                if elem in digits or elem in currency_lst:
                    res_lst.append(elem)
            if len(res_lst) == 3:  # jeśli są 2 liczby oraz jeden element zawierający wyrażenie z currency_lst (jak zł, pln itp)
                share_price, volume = price_volume.get_price_and_volume_value_string(self, res_lst, currency_lst)
            else:
                share_price = pd.NA
                volume = pd.NA
        elif len(digits) == 2 and True in int_eval and False in int_eval:  # są dwie liczby w liscie i jedna int druga nie int (czyli float)
            """ zakładamy że cena akcji jest zawsze float a wolumen jest int - nie można kupić pół akcji """
            check_int_1 = is_int(value=digits[0])  #  isinstance(digits[0], int)
            if check_int_1 is True:  # jeśli digits[0] jest int to na 90% jest to cena akcji
                volume = digits[0]
                share_price = digits[1]
            else:
                volume = digits[1]
                share_price = digits[0]
        else:
            share_price = pd.NA
            volume = pd.NA
        return share_price, volume

    def get_price_and_volume_value_string(self, value_lst, currency_lst):  # podstawowe wyciąganie danych z wyrażenia (liczba, liczba, PLN etc)
        idx = -1
        volume = pd.NA

        for i in range(len(value_lst)):
            string = value_lst[i]
            if any(substring in string for substring in currency_lst):  # jeśli z stringu jest jakikolwiek z elementów w liście currency_lst to...
                idx = i
                break

        if idx != -1:
            share_price = value_lst[idx - 1]
            share_price = float(share_price)
        else:
            share_price = pd.NA

        for i in range(len(value_lst)):
            if i not in [idx, idx - 1]:
                volume = final_volume_check(value_lst[i])  #float(value_lst[i])  # volume jest trzecim elementem poza pln i price na liście inf zbiorczych
                break
        return share_price, volume

    def if_both_zeros(self, dig_variable):  # do wyszukiwania po konretnych transakcjach
        """
        do wyszukiwania po konretnych transakcjach - jeśli informacje zbiorcze są równe 0, a nie pd.NA
        :param dig_variable: kolejna tabela z rzędu - wytłumaczenie na początku klasy
        :return: zwracane są dwie wartości - średnia cena i wolumen
        """
        start_index = 'cenaiwolumen'  # bez spacji bo w 'search_for_term' nie bierzemy pod uwagę spacji
        end_index = 'informacjezbiorcze'

        cls = get_value_lst()
        start = cls.search_for_term(start_index, self.table)  # wynikowe wiersze po wyszukiwanym haśle
        end = cls.search_for_term(end_index, self.table)  # wynikowe wiersze po wyszukiwanym haśle

        start_ind = start.index[dig_variable] + 1  #w niektórych tabela plus 1 a w innych nie :/
        end_ind = end.index[dig_variable]

        df = self.table.iloc[start_ind:end_ind]
        df = df.loc[:, ~df.eq('missing').all()]

        if len(df.columns) == 2:  # jeśli mamy tylko dwie kolumny to zakłądamy że pierwsza z nich to price a druga to wolumen
            price_lst = df.iloc[:, 0].tolist()
            vol_lst = df.iloc[:, 1].tolist()
        else:  # jesli nie zadziała to sprawdzamy drugi sposób po nazwach kolumn - wolumen / cena
            vol = start.isin(['wolumen'])
            price = start.isin(['cena'])
            volobj = vol.any()
            priceobj = price.any()
            vol_col = volobj[volobj is True].index[0]
            price_col = priceobj[priceobj is True].index[0]

            vol_lst = self.table.iloc[start_ind: end_ind][vol_col].tolist()
            price_lst = self.table.iloc[start_ind: end_ind][price_col].tolist()

        sum_vol = 0
        sum_value = 0

        if len(vol_lst) == len(price_lst):
            for i in range(len(vol_lst)):
                digit_string = price_lst[i]

                digit_string = re.sub(r'\s+', ' ', digit_string).replace(',', '.')
                PATTERN = r'[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
                rx = re.compile(PATTERN, re.VERBOSE)
                digits = re.findall(rx, digit_string)  # wyszukiwanie wszystkich liczb w wyrażeniu

                price = float(digits[0])
                volume = float(vol_lst[i].replace(' ', ''))

                sum_vol = sum_vol + volume
                sum_value = sum_value + volume * price

        avg_price = sum_value / sum_vol if sum_vol != 0 else 0  # na wypadek gdyby było równe 0
        return round(avg_price, 3), sum_vol
