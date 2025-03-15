
import requests
from bs4 import BeautifulSoup
import sqlite3
import csv
import time
import os
import datetime

# Конфигурация (настройте под свои нужды)
DATABASE_NAME = "flight_delays.db"
CSV_DIRECTORY = "airport_csv"
AIRPORT_CODES = ["JFK", "LAX", "SFO"]  # Пример списка аэропортов. Замените!
WEB_SCRAPING_SOURCE = "flightstats"  # Или "flightradar24", выберите сайт для парсинга

# --- Функции для работы с SQLite ---
def create_database():
    """Создает базу данных SQLite, если она не существует."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_delays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airport_code TEXT,
            flight_number TEXT,
            departure_time TEXT,
            arrival_time TEXT,
            status TEXT,  -- Добавлено поле status
            data_source TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def insert_flight_delay(airport_code, flight_number, departure_time, arrival_time, status, data_source):
    """Добавляет информацию о задержке рейса в базу данных."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO flight_delays (airport_code, flight_number, departure_time, arrival_time, status, data_source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (airport_code, flight_number, departure_time, arrival_time, status, data_source))
    conn.commit()
    conn.close()


# --- Функции для работы с CSV ---
def create_csv_directory():
    """Создает директорию для CSV-файлов, если она не существует."""
    if not os.path.exists(CSV_DIRECTORY):
        os.makedirs(CSV_DIRECTORY)


def write_airport_data_to_csv(airport_code):
    """Записывает данные о задержках рейсов для указанного аэропорта в CSV-файл."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flight_number, departure_time, arrival_time, status, data_source
        FROM flight_delays
        WHERE airport_code = ?
    """, (airport_code,))
    data = cursor.fetchall()
    conn.close()

    csv_file_path = os.path.join(CSV_DIRECTORY, f"{airport_code}_delays.csv")
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Flight Number", "Departure Time", "Arrival Time", "Status", "Data Source"])  # Заголовок
        csv_writer.writerows(data)  # Данные

    print(f"Данные для аэропорта {airport_code} сохранены в {csv_file_path}")

# --- Функции для получения данных (WEB SCRAPING) ---

def get_flightstats_flight_status(airport_code):
    """Парсит данные о статусе рейсов с FlightStats."""

    #  ВАЖНО:  Сайты меняют структуру!  Убедитесь, что URL и селекторы актуальны!
    url = f"https://www.flightstats.com/v2/airport/status/{airport_code}/arr"  # Пример для прибывающих рейсов, проверьте актуальность URL

    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # ЭТО КЛЮЧЕВОЕ МЕСТО, требующее адаптации под структуру FlightStats!
        #  Inspect HTML исходный код страницы FlightStats и адаптируйте этот код.
        #  Пример (нужно адаптировать):
        flight_table = soup.find('div', {'class': 'table-responsive'})  # Общий контейнер таблицы. Адаптируйте!
        if flight_table:
            flight_rows = flight_table.find_all('div', {'class': 'row'})  # Адаптируйте селектор для каждой строки
            for row in flight_rows:
                try: #Добавим трай кэтч для обработки каждой строки.

                    flight_number_element = row.find('div', {'class': 'flightNumber'}) #Замените на реальный класс. Или ищите по другим атрибутам
                    departure_time_element = row.find('div', {'class': 'depTime'}) #Замените на реальный класс
                    arrival_time_element = row.find('div', {'class': 'arrTime'}) #Замените на реальный класс
                    status_element = row.find('div', {'class': 'status'}) #Замените на реальный класс


                    flight_number = flight_number_element.text.strip() if flight_number_element else "N/A"
                    departure_time = departure_time_element.text.strip() if departure_time_element else "N/A"
                    arrival_time = arrival_time_element.text.strip() if arrival_time_element else "N/A"
                    status = status_element.text.strip() if status_element else "N/A"

                    insert_flight_delay(airport_code, flight_number, departure_time, arrival_time, status, "FlightStats Web")
                except Exception as e:
                    print(f"Ошибка при обработке строки таблицы: {e}")
                    continue #Переходим к следующей строке

        else:
            print(f"Таблица рейсов не найдена на странице FlightStats для {airport_code}")

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к FlightStats: {e}")
    except Exception as e:
        print(f"Ошибка при парсинге FlightStats: {e}")



def get_flightradar24_flight_status(airport_code):
    """Парсит данные о статусе рейсов с FlightRadar24."""
    url = f"https://www.flightradar24.com/data/airports/{airport_code}"  # Пример, может потребоваться адаптация

    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Здесь нужно написать код для извлечения данных из HTML-структуры сайта
        #  Это ОЧЕНЬ сложно без знания структуры FlightRadar24! Сайты часто меняют структуру.
        # Пример (очень упрощенный и, скорее всего, нерабочий):
        # table = soup.find('table', {'class': 'arrivals'}) # Пример класса
        # for row in table.find_all('tr'):
        #   cells = row.find_all('td')
        #   if len(cells) > 5:
        #     flight_number = cells[0].text.strip()
        #     departure_time = cells[1].text.strip()
        #     arrival_time = cells[2].text.strip()
        #     status = cells[3].text.strip()
        #     insert_flight_delay(airport_code, flight_number, departure_time, arrival_time, status, "FlightRadar24 Web")

        print(f"Парсинг FlightRadar24 для {airport_code} требует доработки кода!")  # ВАЖНО!

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к FlightRadar24: {e}")
    except Exception as e:
        print(f"Ошибка при парсинге FlightRadar24: {e}")



# --- Основной код ---

if __name__ == "__main__":
    create_database()
    create_csv_directory()

    for airport_code in AIRPORT_CODES:
        print(f"Сбор данных для аэропорта: {airport_code}")

        if WEB_SCRAPING_SOURCE == "flightstats":
            get_flightstats_flight_status(airport_code)
        elif WEB_SCRAPING_SOURCE == "flightradar24":
            get_flightradar24_flight_status(airport_code) # Помните о необходимости доработки парсера!
        else:
            print("Неверно указан WEB_SCRAPING_SOURCE. Используйте 'flightstats' или 'flightradar24'.")
            exit()

        write_airport_data_to_csv(airport_code)
        time.sleep(10)  # Увеличиваем паузу, так как бесплатный парсинг может быть медленнее и чувствительнее к блокировкам.

    print("Сбор данных завершен.")
