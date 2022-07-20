import time
from insiders_pdf.pdf_files import report_data, alerts_table


''' działanie skryptu do pdfów:

1. odczytywany jest id z tabeli config
2. pobierane są raporty od tego id w górę z kategorią transkacje na akcjach
3. id w bazie config aktualizujemy po każdej iteracji

4. raporty z transakcjami są unikalne pod względem id raportu, a w tabeli rejected i scanned po hashu

Później z tego miejsca odpalany będzie też skrypt do odczytywania wartości na skanach pdfów - z datą i godziną

Usługa do wykrywania tabel w pdfach: https://extracttable.com
'''



start = time.time()

cls = report_data()
cls.get_pdfs()

# try:
#     cls = report_data()
#     cls.get_pdfs()
# except Exception as e:
#     info = f"""Ogólny błąd przy pobieraniu danych z pdfów - sprawdzić ostatnie pliki. Możliwe, że wyskoczył inny błąd
#                 niż te przewidziane w try w funkcji get_pdfs w skrypcie pdf_files.py, error: {e}"""
#     alerts_table(info)

end = time.time()
print("this was %s second" % (round(end - start, 3)))








# table_as_jpg_lst = ['https://biznes.pap.pl/pl/news/espi/attachment/39992364,zawiadomienie_19MAR.pdf,20210804/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/39992606,TIPECA_-_Notification_to_CP.pdf,20210804/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/39925224,DK_NotyfikacjaArt19MAR_26_07_2021.pdf,20210728/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40316026,20210914_181644_0000490149_202109141520-1ADSfz.pdf,20210914/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40324120,20210915_151624_0000490205_1_1b_Zalacznik_2_Powiadomienie_notyfikacyjne_PS..pdf,20210915/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40316014,20210914_181330_0000490148_2021091415208krzb.pdf,20210914/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40308172,202109131648.pdf,20210913/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40304327,Maciej_Kalita_-_MAR_art_19.pdf,20210913/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40279281,202109091715.pdf,20210909/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40221922,2021_09_02_RB34_Zalacznik_-_powiadomienie.pdf,20210902/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40214291,KL_MAR__V1.pdf,20210901/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40209108,ZgloszenieTransakcjiGPW_20210831-RD.pdf,20210831/',  #tricky one
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40198204,Powiadomienie_o_transakcjach.pdf,20210830/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40195807,Powiadomienie_o_transakcji_MKiersznicki.pdf,20210830/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40148142,Powiadomienia_19_ust._1_MAR_Tonski19082021.pdf,20210824/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40115268,382_2021.pdf,20210819/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40096513,378_2021_zawiadomienie_WO.pdf,20210817/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40096303,Zalacznik_nr_3.pdf,20210817/',
#                     'https://biznes.pap.pl/pl/news/espi/attachment/40096303,Zalacznik_nr_1.pdf,20210817/'
#                     ]
