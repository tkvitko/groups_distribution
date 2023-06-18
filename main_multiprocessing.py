import concurrent.futures
import os

from main import (Data,
                  config,
                    WORK_DIR,
                  SOURCE_FILENAME,
                  RESULT_FILENAME,
                  REMOVED_ITEMS_FILENAME)

TEMP_FILE = 'processes_cache'


def job(number):
    data = Data(source_file=SOURCE_FILENAME)

    while data.has_group_with_rating_less_then_requested:

        for i in range(int(config['shuffler']['tries_number'])):
            data.shuffle_and_get_quality()
            if not data.has_group_with_rating_less_then_requested:
                data.save_data_to_excel(number)
                deviation = data.get_max_feedback_count_deviation()
                with open(TEMP_FILE, 'a', encoding='utf-8') as f:
                    f.write(f'{number}:{deviation}\n')
                break
        else:
            data.remove_the_worst_item()


if __name__ == '__main__':

    MAX_WORKERS = 8
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures_dict = {executor.submit(job, i): i for i in range(MAX_WORKERS)}
        for future in concurrent.futures.as_completed(futures_dict):
            future.result()

    with open(TEMP_FILE, encoding='utf-8') as f:
        results = f.readlines()

    min_deviation = 100
    best_number = None
    for result in results:
        number, deviation = result.split(":")
        deviation = float(deviation)
        if min_deviation > deviation:
            min_deviation = deviation
            best_number = number

    for file in os.listdir(WORK_DIR):
        for name in (RESULT_FILENAME.split('/')[-1], REMOVED_ITEMS_FILENAME.split('/')[-1]):
            if name in file and best_number not in file:
                os.remove(os.path.join(WORK_DIR, file))
                print(f'{file} removed')

    os.remove(TEMP_FILE)
    print(f'{TEMP_FILE} removed')

