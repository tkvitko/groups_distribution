import configparser
import os
import random

import pandas as pd

MAX_TRIES = 10000
PERCENTAGE_TO_FIND_THE_WORSE_ELEMENT = 10
RESULTS_NUMBER_TO_CHOOSE_FROM = 3
RATING_STRING = 'Рейтинг'
FEEDBACKS_STRING = 'Кол-во отзывов у товара'

WORK_DIR = 'data'
SOURCE_FILENAME = os.path.join(WORK_DIR, 'input.xlsx')
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input1.xlsx')
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input2.xlsx')
# SOURCE_FILENAME = os.path.join(WORK_DIR, 'input3.xlsx')
RESULT_FILENAME = os.path.join(WORK_DIR, 'артикулы.xlsx')
REMOVED_ITEMS_FILENAME = os.path.join(WORK_DIR, 'исключенные элементы.xlsx')


class NoElements(Exception):
    pass


class Data:
    def __init__(self, source_file):

        # get data from source Excel
        self.source_file = source_file
        items_df = pd.read_excel(source_file)
        self.data = items_df.to_dict(orient='records')

        # get data from config
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.items_per_group = int(config['shuffler']['items_per_group'])
        self.min_rating = float(config['shuffler']['rating'])
        self.groups_number = len(self.data) // self.items_per_group + 1

        # set init values
        self.groups_ratings = {}
        # self.groups_feedback_sigma = {}
        self.has_group_with_rating_less_then_requested = True
        self.removed_items = []
        self.results = []

    def _set_random_groups(self):
        """
        Функция установки случайных групп:
        - меняет порядок элементов на случайный
        - проставляет группы по новому (случайному) порядку с учетом величины группы
        :return:
        """

        random.shuffle(self.data)
        start = 0
        end = self.items_per_group
        for group in range(self.groups_number):
            for item in self.data[start:end]:
                item['group'] = group
            start += self.items_per_group
            end += self.items_per_group

    def _update_groups_rating(self):
        """
        Пересчитывает рейтинг текущих групп
        :return:
        """

        for group in range(self.groups_number):
            group_items = [item for item in self.data if item['group'] == group]

            numerator = sum(
                float(item[RATING_STRING]) * int(item[FEEDBACKS_STRING]) for item in group_items if item[RATING_STRING])
            denominator = sum(int(item[FEEDBACKS_STRING]) for item in group_items if item[RATING_STRING])
            self.groups_ratings[group] = numerator / denominator if denominator != 0 else 0

    def update_groups_feedback_statistics(self, step: int):
        """
        Пересчитывает статистику по количеству отзывов для каждой группы (среднее и максимальное отклонение)
        :return:
        """

        for group in range(self.groups_number):
            group_items = [item for item in self.data if item['group'] == group]
            denominator = sum(int(item[FEEDBACKS_STRING]) for item in group_items if item[RATING_STRING])

            group_average_feedback_count = denominator / len(group_items)
            group_feedback_sigma = 0
            for item in group_items:
                feedback_difference = abs(item[FEEDBACKS_STRING] - group_average_feedback_count)
                if group_feedback_sigma < feedback_difference:
                    group_feedback_sigma = feedback_difference
            self.results[step]['groups'][group] = {'average_feedback_count':group_average_feedback_count,
                                                 'maximum_feedback_sigma': group_feedback_sigma}

    def _update_error(self):
        """
        Обновляет данные о текущей ошибке.
        Если рейтинг хотя бы одной группы ниже требуемого, ошибка всё ещё есть.
        :return:
        """

        for group_rating in self.groups_ratings.values():
            print(group_rating)
            if group_rating < self.min_rating:
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

    def remove_the_worst_item(self):
        """
        Метод поиска и удаления самого плохого элемента.
        - берется 10% (PERCENTAGE_TO_FIND_THE_WORSE_ELEMENT) товаров с самым низким рейтингом,
        - среди них берется 1 с максимальным количеством отзывов.
        :return:
        """

        data_filtered = list(filter(lambda d: d[RATING_STRING] != 0, self.data))
        data_sorted = sorted(data_filtered, key=lambda d: d[RATING_STRING], reverse=True)
        items_with_bad_rating_count = len(data_filtered) * PERCENTAGE_TO_FIND_THE_WORSE_ELEMENT // 100 + 1
        the_worse_items = data_sorted[-items_with_bad_rating_count:]
        try:
            the_worst_item = max(the_worse_items, key=lambda d: d[FEEDBACKS_STRING])
            self.data.remove(the_worst_item)
            self.removed_items.append(the_worst_item)
            return the_worst_item
        except ValueError:
            raise NoElements

    def save_data_to_excel(self):
        """
        Сохранение артикулов и исключенных элементов в Excel-файлы.
        :return:
        """

        items_df = pd.DataFrame.from_dict(self.data)
        removed_df = pd.DataFrame.from_dict(self.removed_items)
        items_df.to_excel(RESULT_FILENAME)
        removed_df.to_excel(REMOVED_ITEMS_FILENAME)


if __name__ == '__main__':
    data = Data(source_file=SOURCE_FILENAME)

    for step in range(RESULTS_NUMBER_TO_CHOOSE_FROM):

        while data.has_group_with_rating_less_then_requested:

            for i in range(MAX_TRIES):
                data.shuffle_and_get_quality()
                if not data.has_group_with_rating_less_then_requested:
                    data.results.append({'step': step, 'data': data.data})
                    data.update_groups_feedback_statistics(step=step)
                    print(f'Итоговые рейтинги групп: {data.groups_ratings}')
                    print(f'Количество элементов, оставшихся после удаления плохих: {len(data.data)}')
                    break
            else:
                try:
                    the_worse_item = data.remove_the_worst_item()
                    print(f'Удален элемент: {the_worse_item}')
                except NoElements:
                    print('Не удалось распределить по требуемому рейтингу, попробуйте понизить рейтинг')
                    break

        data.has_group_with_rating_less_then_requested = True

