import cx_Oracle
import pandas as pd

# SQL connection
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns)

pbl_books_query = """select z.za_zapis_id \"rekord_id\", z.za_type \"typ\", rz.rz_rodzaj_id \"rodzaj_zapisu_id\", rz.rz_nazwa \"rodzaj_zapisu\", dz.dz_dzial_id \"dzial_id\", dz.dz_nazwa \"dzial\", to_char(tw.tw_tworca_id) \"tworca_id\", tw.tw_nazwisko \"tworca_nazwisko\", tw.tw_imie \"tworca_imie\", to_char(a.am_autor_id) \"autor_id\", a.am_nazwisko \"autor_nazwisko\", a.am_imie \"autor_imie\", z.za_tytul \"tytul\", z.za_opis_wspoltworcow \"wspoltworcy\", fo.fo_nazwa \"funkcja_osoby\", to_char(os.os_osoba_id) \"wspoltworca_id\", os.os_nazwisko \"wspoltworca_nazwisko\", os.os_imie \"wspoltworca_imie\", z.za_adnotacje \"adnotacja\", w.wy_nazwa \"wydawnictwo\", w.wy_miasto \"miejscowosc\", z.za_rok_wydania \"rok_wydania\", z.za_opis_fizyczny_ksiazki \"opis_fizyczny\", z.za_uzytk_wpisal, z.za_ro_rok, z.za_tytul_oryginalu, z.za_wydanie, z.za_instytucja, z.za_seria_wydawnicza, z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA

                        from pbl_zapisy z
                        full outer join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
                        full outer join IBL_OWNER.pbl_tworcy tw on zt.zatw_tw_tworca_id=tw.tw_tworca_id
                        full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
                        full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
                        full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zw on zw.zawy_za_zapis_id=z.za_zapis_id 
						full outer join IBL_OWNER.pbl_wydawnictwa w on zw.zawy_wy_wydawnictwo_id=w.wy_wydawnictwo_id
                        full outer join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
                        full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
                        full outer join IBL_OWNER.pbl_udzialy_osob uo on uo.uo_za_zapis_id = z.za_zapis_id
                        full outer join IBL_OWNER.pbl_osoby os on uo.uo_os_osoba_id=os.os_osoba_id
                        full outer join IBL_OWNER.pbl_funkcje_osob fo on fo.fo_symbol=uo.uo_fo_symbol
						            where (z.za_status_imp is null OR z.za_status_imp like 'IOK')
						            and z.za_type like 'KS'"""

pbl_books = pd.read_sql(pbl_books_query, con=connection)