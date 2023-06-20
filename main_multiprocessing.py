import concurrent.futures

from main import *

MAX_WORKERS = 8
INPUT_FOLDER = os.path.join(WORK_DIR, 'input')
DONE_FOLDER = os.path.join(WORK_DIR, 'done')
TEMP_FILE = 'processes_cache'
ERROR_FILE = 'errors.txt'


def job(input_file: str, job_number: int):
    """
    Задача выполнения одного распределения для одного входного файла
    :param job_number: номер задачи
    :return:
    """

    data = Data(source_file=os.path.join(INPUT_FOLDER, input_file))

    while data.has_group_with_rating_less_then_requested:

        for i in range(int(config['shuffler']['tries_number'])):
            data.shuffle_and_get_quality()
            if not data.has_group_with_rating_less_then_requested:
                data.save_data_to_excel(input_file, job_number)
                deviation = data.get_max_feedback_count_deviation()
                with open(TEMP_FILE, 'a', encoding='utf-8') as f:
                    f.write(f'{job_number}:{deviation}\n')
                return True
        else:
            try:
                data.remove_the_worst_item()
            except NoElements:
                with open(ERROR_FILE, 'a', encoding='utf-8') as f:
                    f.write(f'{input_file}\n')
                    return False


def multi_job(input_file: str):
    """
    Задача выполнения N задач (параллельно на MAX_WORKERS ядрах)
    :return:
    """

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures_dict = {executor.submit(job, input_file, i): i for i in range(MAX_WORKERS)}
        for future in concurrent.futures.as_completed(futures_dict):
            has_result = future.result()

    if has_result:

        # чтение результатов работы
        with open(TEMP_FILE, encoding='utf-8') as f:
            results = f.readlines()

        # поиск лучшего результата
        min_deviation = 100
        best_number = None
        for result in results:
            print(result.strip())
            number, deviation = result.split(":")
            deviation = float(deviation)
            if min_deviation > deviation:
                min_deviation = deviation
                best_number = number

        os.remove(TEMP_FILE)
        # print(f'removed: {TEMP_FILE}')

        # удаление всех результатов, кроме лучшего
        os.chdir(WORK_DIR)
        for file in os.listdir('.'):
            if input_file in file:
                if best_number not in file:
                    os.remove(file)
                    # print(f'removed: {file}')
                else:
                    os.rename(file, file.replace(best_number, ''))
                    # print(f'renamed: {file}')

        os.chdir('..')


def read_input_files():
    """
    Функция чтения списка файлов для обработки
    :return: список имен файлов
    """
    input_dir = os.path.join(WORK_DIR, 'input')
    return os.listdir(input_dir)


if __name__ == '__main__':

    for file in read_input_files():
        print(f'start {file}')
        multi_job(input_file=file)

        # перенос файла-исходника из input в done
        os.replace(os.path.join(INPUT_FOLDER, file),
                   os.path.join(DONE_FOLDER, file))
