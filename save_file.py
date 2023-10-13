import os
import json


def save_txt_file(file_name: str, var, rec_mode: str):
    """
    Функция записи данных в файл
    :param file_name: имя файл со спецификацией (имя_файла.txt)
    :param var: передаваемый объект сохранения
    :param rec_mode: режим записи
    :return:
    """
    with open(file_name, rec_mode) as code:
        code.write(var)


def save_json(file_name: str, dct: dict):
    """
    Сохраняет словарь в json
    :param file_name: имя файла
    :param dct: сохраняемый словарь
    :return:
    """
    with open(file_name, 'w', encoding='utf-8') as js:
        json.dump(dct, js, indent=4, ensure_ascii=False)


def read_txt_file(file_name: str) -> list:
    """
    Считывает переданный файл
    :param file_name: имя файла
    :return: список кодов
    """
    with open(file_name, 'r') as fl:
        file = fl.read().split()
    return file


def read_json_file(file_name: str) -> dict:
    """
    Читаем переданный json файл
    :param file_name: имя файла
    :return: словарь
    """
    with open(file_name, 'r', encoding='utf-8') as fl:
        return json.load(fl)