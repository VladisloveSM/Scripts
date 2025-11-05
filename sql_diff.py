import cx_Oracle
import csv
from datetime import datetime

# === НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ===
db1_config = {
    "user": "test_user1",
    "password": "test_password1",
    "dsn": "test_dsn1"
}

db2_config = {
    "user": "test_user2",
    "password": "test_password2",
    "dsn": "test_dsn2"
}

# === ВХОДНЫЕ ДАННЫЕ ===
table_name = 'SCWEB_SCRIPTS'
pk_columns = ['ID', 'PRIVILEGE_ID']
remove_spaces = True 

# === ФУНКЦИИ ===
def fetch_all_rows(conn_config, table_name):
    conn = cx_Oracle.connect(**conn_config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [col[0].upper() for col in cursor.description]

    rows = []
    for r in cursor:
        row_dict = {}
        for col_name, value in zip(columns, r):
            if isinstance(value, cx_Oracle.LOB):
                try:
                    value = value.read()
                except Exception:
                    value = None
            row_dict[col_name] = value
        rows.append(row_dict)

    cursor.close()
    conn.close()
    return rows


def make_pk_key(row, pk_columns):
    return tuple(row[col] for col in pk_columns)


def normalize_value(val):
    """Нормализация строк, если remove_spaces=True"""
    if val is None:
        return None
    if remove_spaces and isinstance(val, str):
        return ''.join(val.split())  # убираем пробелы, табы, \n, \r
    return val

# === ОСНОВНАЯ ЛОГИКА ===
print("Считываем данные из первой базы...")
rows_db1 = fetch_all_rows(db1_config, table_name)

print("Считываем данные из второй базы...")
rows_db2 = fetch_all_rows(db2_config, table_name)

map_db1 = {make_pk_key(r, pk_columns): r for r in rows_db1}
map_db2 = {make_pk_key(r, pk_columns): r for r in rows_db2}

diffs = []
missing_in_db2 = []
missing_in_db1 = []

for pk, row1 in map_db1.items():
    if pk not in map_db2:
        missing_in_db2.append(pk)
    else:
        row2 = map_db2[pk]
        for col in row1.keys():
            val1 = normalize_value(row1[col])
            val2 = normalize_value(row2[col])
            if val1 != val2:
                diffs.append({
                    "PK": pk,
                    "COLUMN": col,
                    "DB1_VALUE": row1[col],
                    "DB2_VALUE": row2[col]
                })

for pk in map_db2.keys():
    if pk not in map_db1:
        missing_in_db1.append(pk)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfile = f"diff_report_{table_name}_{timestamp}.csv"

with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Тип", "Первичный ключ", "Колонка", "DB1", "DB2"])
    for d in diffs:
        writer.writerow(["DIFF", d["PK"], d["COLUMN"], d["DB1_VALUE"], d["DB2_VALUE"]])
    for pk in missing_in_db2:
        writer.writerow(["MISSING_IN_DB2", pk, "", "", ""])
    for pk in missing_in_db1:
        writer.writerow(["MISSING_IN_DB1", pk, "", "", ""])

print(f"\n✅ Сравнение завершено. Результат сохранён в файл: {outfile}")