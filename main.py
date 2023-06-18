import configparser
import os
import random

import pandas as pd

RATING_STRING = 'Рейтинг'
FEEDBACKS_STRING = 'Кол-во отзывов у товара'

WORK_DIR = 'data'
SOURCE_FILENAME = os.path.join(WORK_DIR, 'input.xlsx')    # production
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input1.xlsx')   # 66 шт с нулевыми
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input2.xlsx')   # 227 шт с нулевыми
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input3.xlsx')   # 162 шт без 0
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input4.xlsx')
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input5.xlsx')  # 696 с нулевыми
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input6.xlsx')  # input5 без нулевых
RESULT_FILENAME = os.path.join(WORK_DIR, 'result')
REMOVED_ITEMS_FILENAME = os.path.join(WORK_DIR, 'deleted')

config = configparser.ConfigParser()
config.read("config.ini")


class NoElements(Exception):
    pass


class Data:
    def __init__(self, source_file):

        # чтение данных из входного Excel
        self.source_file = source_file
        items_df = pd.read_excel(source_file)
        self.data = items_df.to_dict(orient='records')

        # вынос товаров с нулевым рейтингом из расчета
        # self.data_source = items_df.to_dict(orient='records')
        # self.data_with_rating = [item for item in self.data_source if item[RATING_STRING]]
        # self.data_with_null_rating = [item for item in self.data_source if not item[RATING_STRING]]

        # возврат части товаров с нулевым рейтингом в расчеты
        # percentage_to_get_nulls_to_data = 30
        # number_of_nulls_to_get = len(self.data_with_null_rating) * percentage_to_get_nulls_to_data // 100
        # for item in self.data_with_null_rating[:number_of_nulls_to_get]:
        #     self.data_with_null_rating.remove(item)
        #     self.data_with_rating.append(item)

        # чтение параметров из конфига
        # (!) избыточные вычисления для попытки исключения товаров с нулевым рейтингом из расчетов (отключена выше)
        self.items_per_group_requested = int(config['shuffler']['items_per_group'])
        self.min_rating_requested = float(config['shuffler']['rating'])
        self.groups_number = len(self.data) // self.items_per_group_requested + 1
        self.items_with_rating_per_group = len(self.data) // self.groups_number + 1

        # дополнение набора данных коэффициентом вреда
        for item in self.data:
            item['badness'] = (self.min_rating_requested - int(item[RATING_STRING])) * int(item[FEEDBACKS_STRING])

        # установка начальных значений
        self.groups_ratings = {}
        self.has_group_with_rating_less_then_requested = True
        self.removed_items = []
        # self.groups_feedback_sigma = {}

    def _set_random_groups(self):
        """
        Функция установки случайных групп:
        - меняет порядок элементов на случайный
        - проставляет группы по новому (случайному) порядку с учетом величины группы
        :return:
        """

        random.shuffle(self.data)
        start = 0
        end = self.items_with_rating_per_group
        for group in range(self.groups_number):
            for item in self.data[start:end]:
                item['group'] = group
            start += self.items_with_rating_per_group
            end += self.items_with_rating_per_group

    def _update_groups_rating(self):
        """
        Пересчитывает рейтинг текущих групп и
        статистику по количеству отзывов для каждой группы (среднее и максимальное отклонение)
        :return:
        """

        for group in range(self.groups_number):
            group_items = [item for item in self.data if item['group'] == group]

            # средний рейтинг группы
            if group_items:
                numerator = sum(
                    float(item[RATING_STRING]) * int(item[FEEDBACKS_STRING]) for item in group_items if
                    item[RATING_STRING])
                denominator = sum(int(item[FEEDBACKS_STRING]) for item in group_items if item[RATING_STRING])
                self.groups_ratings[group] = numerator / denominator if denominator != 0 else 0
            else:
                if group in self.groups_ratings.keys():
                    self.groups_ratings.pop(group)

    def get_max_feedback_count_deviation(self):
        # статистика по количеству отзывов

        summ_deviation = 0

        for group in self.groups_ratings.keys():
            group_items = [item for item in self.data if item['group'] == group and item[RATING_STRING]]
            denominator = sum(int(item[FEEDBACKS_STRING]) for item in group_items)
            group_average_feedback_count = denominator / len(group_items)
            group_max_feedback_deviation = 0
            for item in group_items:
                feedback_deviation = abs(int(item[FEEDBACKS_STRING]) - group_average_feedback_count)
                if group_max_feedback_deviation < feedback_deviation:
                    group_max_feedback_deviation = feedback_deviation
            summ_deviation += group_max_feedback_deviation

        return summ_deviation / len(self.groups_ratings)

    def _update_error(self):
        """
        Обновляет данные о текущей ошибке.
        Если рейтинг хотя бы одной группы ниже требуемого, ошибка всё ещё есть.
        :return:
        """
        # print(self.groups_ratings)
        # print(sum(self.groups_ratings.values()) / len(self.groups_ratings))
        for group_rating in self.groups_ratings.values():
            if group_rating < self.min_rating_requested:
                self.has_group_with_rating_less_then_requested = True
                break
        else:
            self.has_group_with_rating_less_then_requested = False

    def shuffle_and_get_quality(self):
        """
        Метод для запуска очередного распределения.
        :return:
        """

        self._set_random_groups()
        self._update_groups_rating()
        self._update_error()

    # def remove_the_worst_item(self):
    #     """
    #     Метод поиска и удаления самого плохого элемента.
    #     - берется 10% (percentage_to_find_the_worst_element) товаров с самым низким рейтингом,
    #     - среди них берется 1 с максимальным количеством отзывов.
    #     :return:
    #     """
    #
    #     percentage = int(config['shuffler']['percentage_to_find_the_worst_element'])
    #     data_filtered = list(filter(lambda d: d[RATING_STRING] != 0, self.data_with_rating))
    #     data_sorted = sorted(data_filtered, key=lambda d: d[RATING_STRING], reverse=True)
    #     items_with_bad_rating_count = len(data_filtered) * percentage // 100 + 1
    #     the_worst_items = data_sorted[-items_with_bad_rating_count:]
    #
    #     for item in the_worst_items:
    #         print(item)
    #
    #     try:
    #         the_worst_item = max(the_worst_items, key=lambda d: d[FEEDBACKS_STRING])
    #         self.data_with_rating.remove(the_worst_item)
    #         self.removed_items.append(the_worst_item)
    #         return the_worst_item
    #     except ValueError:
    #         raise NoElements

    def remove_the_worst_item(self) -> dict:
        """
        Метод поиска и удаления самого плохого элемента.
        Самым плохим элементом считается тот, у которого самое большое значение по формуле:
        (запрошенный минимальный рейтинг - рейтинг элемента) * количество отзывов
        :return: the worst element
        """

        data_filtered = list(filter(lambda d: d[RATING_STRING] < 4, self.data))
        the_worst_item = max(data_filtered, key=lambda d: d['badness'])
        self.data.remove(the_worst_item)
        self.removed_items.append(the_worst_item)
        return the_worst_item

    def save_data_to_excel(self, number: str|int):
        """
        Сохранение артикулов и исключенных элементов в Excel-файлы.
        :return:
        """

        items_df = pd.DataFrame.from_dict(self.data)
        removed_df = pd.DataFrame.from_dict(self.removed_items)
        items_df.to_excel(f'{RESULT_FILENAME}{number}.xlsx')
        removed_df.to_excel(f'{REMOVED_ITEMS_FILENAME}{number}.xlsx')


if __name__ == '__main__':

    data = Data(source_file=SOURCE_FILENAME)

    while data.has_group_with_rating_less_then_requested:

        for i in range(int(config['shuffler']['tries_number'])):
            data.shuffle_and_get_quality()
            if not data.has_group_with_rating_less_then_requested:
                data.save_data_to_excel(number='')
                print(f'Итоговые рейтинги групп: {data.groups_ratings}')
                print(f'Количество элементов, оставшихся после удаления плохих: {len(data.data)}')
                deviation = data.get_max_feedback_count_deviation()
                print(f'Максимальное отклонение количества отзывов от среднего по группе: {deviation}')
                break
        else:
            the_worst_item = data.remove_the_worst_item()
            print(f'Удален элемент: {the_worst_item}')
