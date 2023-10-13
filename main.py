import os
from datetime import datetime
from dotenv import load_dotenv
from class_thai_parser import Thailand_parser
from save_file import read_json_file


# import export
def main():
    # Проверяем наличие переменных окружения и считываем их
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)

    # Создаем необходимые словари с параметрами
    dct_post_param = {'show_search': 1, 'tab': 'by_country', 'imex_type': 'export', 'year': 2023}
    dct_rename_columns = {"COUNTRY.1": "partner_code",
                          "Quantity": "netweight",
                          f"{'FOB (Baht)' if dct_post_param['imex_type'] == 'export' else 'CIF (Baht)'}": "trade_value"}
    dct_table_name = {'table_point': os.getenv('TABLE_POINT'), 'data_thai_table': os.getenv('DATA_THAI_TABLE')}
    # Экземпляр класса Thailand_parser
    my_instance_parser = Thailand_parser(os.getenv('URL_TNVED_LOAD'), os.getenv('URL_MAIN_FROM'), os.getenv('SQLITE_DB_NAME'),
                                         dct_post_param, dct_table_name)
    # Обновление кодов ТНВЭД, если в json файле указан верное значение
    if read_json_file('json_bool_update_code.json')['Update']:
        my_instance_parser.load_need_tnved_code()

    # Валидация очистки временной таблицы с данными
    # Полная очистка таблицы
    if my_instance_parser.get_value_flag_not_save('code') == '0' and my_instance_parser.get_value_flag_not_save('trade_flow') == 'export':
        my_instance_parser.del_value_in_db(dct_table_name['data_thai_table'], type_flow=1 if dct_post_param['imex_type'] == 'import'
                                           else 2, all_value=True)
    # Очистка только опеределенного кода
    elif my_instance_parser.get_value_flag_not_save('code') != '0':
        my_instance_parser.del_value_in_db(dct_table_name['data_thai_table'], type_flow=1 if dct_post_param['imex_type'] == 'import'
                                           else 2, code=my_instance_parser.get_value_flag_not_save('code'))

    # Считываем названия колонок, которые необходимо заменить и вызываем функцию парсера
    my_instance_parser.main_parser(dct_rename_columns, [])


if __name__ == '__main__':
    main()
