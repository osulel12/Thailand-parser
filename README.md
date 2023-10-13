# Thailand-parser

🗂 Перед началом использования необходимо создать следующие объекты:
* .env с переменными окружения
* 25_apk.txt файл хранящий в себе наименования кодов свыше 24 по 4 знака (при необходимости)
* json_bool_update_code.json файл отвечающий за управление скачивания, со следующей структурой словаря
  - "Update": false - в зависимости от значения будет или не будет производиться загрузка. При первом запуске
  обязательно установить значение *true* для сохранения всех кодов ресурса
* tmp_thai_data.dp временную базу данных sqlite для хранения промежуточного результата

Работу по сбору данных производится в файле **main.py**. За функционал отвечает функция main внутри которой составлены
словари:
* ```python
  dct_post_param = {'show_search': 1, 'tab': 'by_country', 'imex_type': 'import', 'year': 2023}
  ```
  отвечает за набор параметров отправляемых на сервис. 
  - С помощью ключа *imex_type* можно изменить тип скачиваемых данных (импорт или экспорт)
  - С помощью ключа *year* можно указать необходимый год скачивания

* ```python
  dct_table_name = {'table_point': os.getenv('TABLE_POINT'), 'data_thai_table': os.getenv('DATA_THAI_TABLE')}
  ```
  Хранит названия таблиц в БД
* ```python
  dct_rename_columns = {"COUNTRY.1": "partner_code",
                          "Quantity": "netweight",
                          f"{'FOB (Baht)' if dct_post_param['imex_type'] == 'export' else 'CIF (Baht)'}": "trade_value"}
  ```
  Словарь для переименования названий колонок 

⏲ Примерное время выгрузки 3000 кодов по каждому месяцу, каждой стране составляет **8 часов**

## Трансформация данных и загрузка в основную БД

Данный функционал реализован в файле **load in db.ipynb**
Тут потребуется создать **config_js_name_table_by_transform.json** в котором будут размещены названия таблиц, схем и временных баз данных. 
Структура файла:

```python
name_table = {'name_schema_main': '',
       'name_table_main': '',
       'name_schema_reference': '',
       'name_table_reference': '',
       'name_tb_country': '',
       'name_tb_tnved_code': '',
       'name_schema_dl': '',
       'name_table_dl': '',
       'sqlite_db': '',
       'name_tb_sqlite': ''}
```
Все разделы подписаны в файле и настроена навигация.
Для успешного преобразования и загрузки в базу данных понадобятся следующие разделы:
* Сбор курса THB - собираем курс валюты для корректного приведения стоимости к доллару
* Сбор справочника измерений - для унификации единиц измерения
* Словарь стран - для унификации стран партнеров
* Трансформация датафрейма - основные изменения
* Загрузка данных в БД 