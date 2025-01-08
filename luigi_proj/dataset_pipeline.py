import os
import luigi
import requests
import tarfile
import gzip
import shutil
import pandas as pd
import io

# Основной каталог проекта
BASE_DIR = "C:/Users/ivlas/Documents/luigi_proj"
DATA_DIR = os.path.join(BASE_DIR, "data")  # Папка для хранения данных
RAW_DIR = os.path.join(DATA_DIR, "raw")  # Папка для сырых данных (архивы)
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")  # Папка для обработанных данных


# Задача для скачивания датасета
class DownloadDataset(luigi.Task):
    # Параметр для URL, с которого будет скачиваться архив
    url = luigi.Parameter(
        default="https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file"
    )

    # Определение выходного файла для этой задачи
    def output(self):
        return luigi.LocalTarget(os.path.join(RAW_DIR, "GSE68849_RAW.tar"))

    # Загрузка архива с данными
    def run(self):
        os.makedirs(
            RAW_DIR, exist_ok=True
        )  # Создание каталога для хранения архива, если он не существует
        response = requests.get(
            self.url, stream=True
        )  # Загружаем файл с указанного URL
        with open(self.output().path, "wb") as f:
            f.write(response.content)  # Сохраняем содержимое архива в файл


# Задача для распаковки tar-архива
class ExtractTarArchive(luigi.Task):
    def requires(self):
        return (
            DownloadDataset()
        )  # Эта задача зависит от выполнения задачи DownloadDataset

    # Определение выходного файла
    def output(self):
        return luigi.LocalTarget(RAW_DIR)

    # Распаковка архива
    def run(self):
        with tarfile.open(self.input().path, "r") as tar:  # Открываем tar-архив
            tar.extractall(path=RAW_DIR)  # Извлекаем все файлы в папку RAW_DIR


# Задача для обработки файлов из архива
class ProcessFiles(luigi.Task):
    def requires(self):
        return ExtractTarArchive()  # Эта задача зависит от выполнения ExtractTarArchive

    # Определение выходного каталога для обработанных файлов
    def output(self):
        return luigi.LocalTarget(PROCESSED_DIR)

    # Основная логика обработки файлов
    def run(self):
        os.makedirs(
            PROCESSED_DIR, exist_ok=True
        )  # Создаем каталог для обработанных файлов

        # Поиск всех gz-файлов в каталоге RAW_DIR
        gz_files = [
            os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".gz")
        ]

        # Обработка каждого .gz файла
        for gz_file in gz_files:
            # Извлекаем имя файла без расширения .gz
            file_name = os.path.basename(gz_file).replace(".gz", "")
            folder_path = os.path.join(
                PROCESSED_DIR, file_name
            )  # Создаем папку для каждого файла
            os.makedirs(folder_path, exist_ok=True)

            # Распаковка .gz файла
            with gzip.open(gz_file, "rt") as f_in:  # Открываем .gz файл для чтения
                file_path = os.path.join(
                    folder_path, file_name
                )  # Путь для сохранения файла
                with open(file_path, "w") as f_out:  # Открываем файл для записи
                    shutil.copyfileobj(
                        f_in, f_out
                    )  # Копируем содержимое из .gz в обычный файл

            # Обработка текста файла
            self._process_file(file_path)

    # Метод для обработки каждого файла
    def _process_file(self, file_path):
        dfs = {}  # Словарь для хранения датафреймов

        # Чтение содержимого файла
        with open(file_path, "r") as f:
            write_key = None  # Переменная для хранения текущего заголовка секции
            fio = io.StringIO()  # Временный буфер для чтения данных
            for line in f.readlines():  # Читаем файл построчно
                if line.startswith(
                    "["
                ):  # Если строка начинается с '[' — это новый заголовок секции
                    if write_key:
                        fio.seek(0)
                        header = (
                            None if write_key == "Heading" else "infer"
                        )  # Заголовок для таблицы
                        dfs[write_key] = pd.read_csv(
                            fio, sep="\t", header=header
                        )  # Читаем данные в DataFrame
                    fio = io.StringIO()  # Новый буфер для новой секции
                    write_key = line.strip("[]\n")  # Обновляем текущий заголовок
                    continue
                if write_key:
                    fio.write(line)  # Записываем строку в буфер

            fio.seek(0)
            dfs[write_key] = pd.read_csv(
                fio, sep="\t"
            )  # Читаем последнюю секцию после завершения цикла

        # Сохраняем каждый датафрейм в отдельный tsv-файл
        for key, df in dfs.items():
            output_path = os.path.join(
                os.path.dirname(file_path), f"{key}.tsv"
            )  # Формируем путь для файла
            df.to_csv(output_path, sep="\t", index=False)  # Сохраняем в файл

        # Если в данных есть таблица "Probes", выполняем ее дополнительную обработку
        if "Probes" in dfs:
            self._truncate_probes(dfs["Probes"], file_path)

        # Удаляем исходный .txt файл, так как его обработка завершена
        os.remove(file_path)

    # Метод для удаления ненужных колонок в таблице Probes
    def _truncate_probes(self, probes_df, file_path):
        # Список колонок для удаления
        columns_to_remove = [
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]
        truncated_df = probes_df.drop(
            columns=columns_to_remove, errors="ignore"
        )  # Удаляем ненужные колонки
        output_path = os.path.join(
            os.path.dirname(file_path), "Probes_truncated.tsv"
        )  # Путь для сохранения файла
        truncated_df.to_csv(
            output_path, sep="\t", index=False
        )  # Сохраняем урезанную таблицу


# Задача-обертка для запуска всех этапов пайплайна
class DatasetPipeline(luigi.WrapperTask):
    def requires(self):
        return ProcessFiles()  # Запускаем обработку файлов как основной этап пайплайна


# Запуск пайплайна
if __name__ == "__main__":
    luigi.run()  # Запускаем пайплайн через luigi
