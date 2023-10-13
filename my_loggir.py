import logging


class Loggir_for_parser:
    """
    Класс настройки кастомного логира
    """
    def __init__(self, name: str):
        """
        Parameters
        ----------
        name: имя модуля из которого происходит вызов
        """
        self.name = name

    def get_logging(self) -> logging.Logger:
        """

        Returns
        -------
        return: настроенный логер
        """
        # Создаем свой логер
        py_logg = logging.getLogger(self.name)
        # Настраиваем уровень логирования
        py_logg.setLevel(logging.INFO)

        # Куда будут записываться логи + способ записи (добавление)
        log_handler = logging.FileHandler(f'{self.name}.txt', mode='w')
        log_stream = logging.StreamHandler()
        # Формат оторбражения
        log_formatter = logging.Formatter("%(filename)s %(asctime)s %(levelname)s %(message)s",
                                          datefmt='%Y-%m-%d %H:%M')

        # добавление форматировщика к обработчику
        log_handler.setFormatter(log_formatter)
        log_stream.setFormatter(log_formatter)
        # добавление обработчика к логгеру
        py_logg.addHandler(log_handler)
        py_logg.addHandler(log_stream)

        return py_logg
