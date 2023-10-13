import pandas as pd
import sqlite3
from datetime import date
import os
from bs4 import BeautifulSoup
import requests
import time
from save_file import save_json, save_txt_file, read_txt_file
from io import StringIO
from typing import Union
from my_loggir import Loggir_for_parser


class Thailand_parser:
    """Клас реализующий логику сбора статистики импорта и экспорта"""

    def __init__(self, url_load_tnved_code: str, url_form_group: str, sqlite_db: str,
                 dct_post_param: dict, dct_db_name: dict):
        """
        :param url_load_tnved_code: ссылка для загрузки кодов ТНВЭД
        :param url_form_group: ссылка на форму из которой будем получать данные
        :param sqlite_db: путь к нашей базе данных
        :param dct_post_param: словарь параметров для пост запроса
        :param dct_db_name: словарь с названием баз данных и таблиц
        """
        self.url_load_tnved_code = url_load_tnved_code
        self.url_form_group = url_form_group
        self.dct_post_param = dct_post_param
        self.dct_db_name = dct_db_name
        self.connect_sqlite = sqlite3.connect(sqlite_db)
        self.cursor = self.connect_sqlite.cursor()

    def load_need_tnved_code(self, upper_border: int = 25):
        """
        Функция скачивает необходимый файл с кодами ТНВЭ, очищает их
        и сохраняет нужные коды, которые меньше 25 или больше 24, но с меткой apk
        :param upper_border: верхняя граница отсекающая не нужные коды ТНВЭД
        :return:
        """
        # Получаем ссылку на скачиваемый файл, если есть более старая верси, то удаляем ее
        # и сохраняем в новый файл
        load_url = requests.get(self.url_load_tnved_code)
        if os.path.isfile('download_code.txt'):
            os.remove('download_code.txt')

        save_txt_file("download_code.txt", load_url.content, 'wb')

        # Считываем скаченный файд с кодами
        with open('download_code.txt', 'r') as fl:
            file_tnved = fl.readlines()

        # Очищаем коды и записываем в файл
        clean_list_code = []
        for code in file_tnved:
            new_code = ''
            old_code = code[4:]
            for c in old_code:
                if c.isdigit():
                    new_code += c
                else:
                    break
            if int(new_code[:2]) < upper_border or new_code[:4] in read_txt_file('25_apk.txt'):
                clean_list_code.append(new_code)

        save_txt_file("need_tnved_code.txt", ' '.join(clean_list_code), 'w')

    def get_latest_month(self, this_year: str = '2023') -> int:
        """
        Функция создает словарь месяцев и их порядковых номеров, а так же
        возвращает значение последнего актуального месяца
        :param this_year: текущий год
        :return: порядковый номер последнего актуального месяца
        """

        # Получаем разметку страницы
        responce = requests.get(self.url_form_group).text
        soup = BeautifulSoup(responce, 'lxml')

        # Получаем список всех месяцев
        html_by_month = soup.find('select', id='width-dynamic')
        # Получаем форму отправки для поиска актуального месяца
        html_by_max_month = soup.find('table', class_='table table-bordered table-striped')
        # Создаем словарь месяцев
        dct_month_by_thailand = {mh.text.strip(): int(mh['value']) for mh in html_by_month.find_all('option')}
        # Сохраняем словаь в json
        save_json('month_dct.json', dct_month_by_thailand)
        # Поиск нужного месяца в форме отправки и возвращение его порядкового элемента
        for text_td in html_by_max_month.find_all('td'):
            try:
                if this_year in text_td.text:
                    return dct_month_by_thailand[text_td.text.strip().split()[0]]
            except KeyError:
                pass

    def del_value_in_db(self, table_name: str, type_flow: int, code: str = '', all_value: bool = False):
        """
        Очистка таблиц, может быть как полная, так и частичная, если передать параметр code
        :param type_flow: тип удлаяемых данных, импорт или экспорт
        :param table_name: имя очищаемой таблицы
        :param code: наименование кода, который очищаем
        :param all_value: флаг для очистки всей таблицы
        :return:
        """
        sql_del = f"""DELETE FROM {table_name} WHERE commodity_code = '{code}' AND trade_flow_code = {type_flow}"""
        sql_del_all_value = f"""DELETE FROM {table_name}"""
        self.cursor.execute(sql_del_all_value if all_value else sql_del)
        self.connect_sqlite.commit()

    def write_save_point(self, value: str, column: str):
        """
        Обновляет значения точек сохранения
        :param value: валидируемое значение
        :param column: название колонки обновления
        :return:
        """
        sql_query = f"""UPDATE {self.dct_db_name['table_point']} SET {column} = '{value}'"""
        self.cursor.execute(sql_query)
        self.connect_sqlite.commit()

    def get_value_flag_not_save(self, column: str) -> Union[int, str]:
        """
        Функция возвращает необходимы значения точек сохранения, такие как коды
        и тип trade_flow. Необходимо для валидации сбоев и полной очистки временной таблицы с данными
        :param column: название столбца
        :return: значение из столбца, может быть двух типов
        """
        sql_select = f"""SELECT {column} FROM {self.dct_db_name['table_point']}"""
        return self.cursor.execute(sql_select).fetchone()[0]

    def all_close(self):
        """
        Закрываем все подключения
        :return:
        """
        self.connect_sqlite.close()
        self.cursor.close()

    def main_parser(self, dct_rename_col: dict, need_month_list: list):
        """
        Функция парсинга данных
        :param dct_rename_col: словарь с колонками которые нужно переименовать {'старая': 'новая'}
        :param need_month_list: в случае выкачки данных по определенным месяцам передаем их список
        :return:
        """
        # Собираем необходимые ТНВЭД коды
        list_tnved_code = read_txt_file('need_tnved_code.txt')
        code_point = self.get_value_flag_not_save('code')
        list_tnved_code = list_tnved_code[list_tnved_code.index(code_point) if code_point != '0' else 0:]
        # Создаем экземпляр нашего логгера для отображения процесса выполнения и валидации ошибок
        lg = Loggir_for_parser(__name__).get_logging()
        len_list_tnved = len(list_tnved_code)
        # Получаем последний актуальный месяц текущего года
        latest_month = self.get_latest_month()
        # В зависимости от типа импорта или экспорта подставляем нужное название колонки
        need_value_columns = 'CIF (Baht)' if self.dct_post_param['imex_type'] == 'import' else 'FOB (Baht)'
        # Начинаем обходить каждый код
        for idx, code in enumerate(list_tnved_code):
            lg.info(f'Осталось кодов {len_list_tnved - idx}')
            self.dct_post_param['tariff_code'] = code
            self.write_save_point(code, 'code')
            # Собираем список необходимых месяцев
            var_itter_month = need_month_list if need_month_list else range(1, latest_month + 1
                                                            if date.today().year == self.dct_post_param['year'] else 13)
            # Начинаем обходить каждый месяц для каждого кода
            for month in var_itter_month:
                flag_done_write = True
                self.dct_post_param['month'] = month
                while flag_done_write:
                    try:
                        # Получаем данные формы и обрабатываем ее
                        response_data = requests.post(self.url_form_group, data=self.dct_post_param, timeout=15)
                        # Создаем наш временный датафрейм и добавляем в него необходимые колонки
                        df_tmp = pd.read_html(StringIO(response_data.text))[2].droplevel(level=0, axis=1).iloc[:, :4] \
                                              .drop(columns=['COUNTRY'])[:-1]
                        df_tmp = df_tmp[df_tmp.apply(lambda x: x['Quantity'] > 0 or x[need_value_columns] > 0, axis=1)]
                        if df_tmp.shape[0] == 0:
                            lg.info(f'Данные по {code} с месяцем {month} отсутствуют')
                            break
                        df_tmp.rename(columns=dct_rename_col, inplace=True)
                        df_tmp['commodity_code'] = code
                        df_tmp['aggregate_level'] = len(code)
                        df_tmp['year'] = self.dct_post_param['year']
                        df_tmp['period'] = f"{self.dct_post_param['year']}-{f'0{month}' if month < 10 else month}-01"
                        df_tmp['trade_flow_code'] = 1 if self.dct_post_param['imex_type'] == 'import' else 2
                        df_tmp['reporter_code'] = 764
                        df_tmp['classification'] = 'HS'
                        df_tmp['region_code'] = 'NNNNN'
                        df_tmp['customs_proc_code'] = 'C00'
                        df_tmp['qty_unit_code'] = 8
                        df_tmp['qty'] = 0
                        df_tmp['flag'] = 0
                        df_tmp['plus'] = 0
                        df_tmp['load_mark'] = 1
                        df_tmp['update_date'] = date.today().strftime('%Y-%m-%d')
                        df_tmp['description'] = ' ** '.join([i for i in pd.read_html(StringIO(response_data.text))[1][2][2:] if isinstance(i, str)])
                        # Записываем наши данные в промежуточную таблицу
                        df_tmp.to_sql(self.dct_db_name['data_thai_table'], con=self.connect_sqlite, if_exists='append',
                                      index=False)
                        lg.info(f'{code} с месяцем {month} успешно загружен в базу')
                        flag_done_write = False
                        time.sleep(2)
                    except (Exception, requests.exceptions.Timeout):
                        lg.exception(f'Ошибка с обработкой датафрейма, код {code} месяц {month}')
                        flag_done_write = True
                        time.sleep(120)
            # Меняем точку сохранения на 0, тем самым понимаем, что код выгружен корректно
            self.write_save_point('0', 'code')
        # Для валидация полного цикла загрузки (импорт и экспорт)
        self.write_save_point(self.dct_post_param['imex_type'], 'trade_flow')
        self.all_close()