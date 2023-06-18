import concurrent.futures

from main import *

TEMP_FILE = 'processes_cache'


def job(job_number: int):
    data = Data(source_file=SOURCE_FILENAME)

    while data.has_group_with_rating_less_then_requested:

        for i in range(int(config['shuffler']['tries_number'])):
            data.shuffle_and_get_quality()
            if not data.has_group_with_rating_less_then_requested:
                data.save_data_to_excel(job_number)
                deviation = data.get_max_feedback_count_deviation()
                with open(TEMP_FILE, 'a', encoding='utf-8') as f:
                    f.write(f'{job_number}:{deviation}\n')
                break
        else:
            data.remove_the_worst_item()


if __name__ == '__main__':

    MAX_WORKERS = 8
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures_dict = {executor.submit(job, i): i for i in range(MAX_WORKERS)}
        for future in concurrent.futures.as_completed(futures_dict):
            future.result()

    # чтение результатов работы
    with open(TEMP_FILE, encoding='utf-8') as f:
        results = f.readlines()

    # поиск лучшего результата
    min_deviation = 100
    best_number = None
    for result in results:
        print(result)
        number, deviation = result.split(":")
        deviation = float(deviation)
        if min_deviation > deviation:
            min_deviation = deviation
            best_number = number

    os.remove(TEMP_FILE)
    print(f'removed: {TEMP_FILE}')

    # удаление всех результатов, кроме лучшего
    os.chdir(WORK_DIR)
    for file in os.listdir('.'):
        for name in (os.path.split(RESULT_FILENAME)[-1], os.path.split(REMOVED_ITEMS_FILENAME)[-1]):
            if name in file:
                if best_number not in file:
                    os.remove(file)
                    print(f'removed: {file}')
                else:
                    os.rename(file, file.replace(best_number, ''))
                    print(f'renamed: {file}')
