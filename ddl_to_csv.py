import re
import csv

# Путь к входному DDL-файлу
ddl_file_path = 'schema.sql'
output_csv_path = 'table_structure.csv'

# Регулярные выражения
table_regex = re.compile(r'CREATE TABLE\s+(\S+)\s*\((.*?)\);', re.DOTALL | re.IGNORECASE)
column_regex = re.compile(
    r'^\s*([a-zA-Z0-9_"]+)\s+([a-zA-Z0-9_\(\),\s]+?)(NOT NULL|NULL)?(?:,|\n|$)', 
    re.MULTILINE | re.IGNORECASE
)

structure = []

# Чтение DDL-файла
with open(ddl_file_path, 'r', encoding='utf-8') as f:
    ddl_content = f.read()

# Поиск таблиц и колонок
tables = table_regex.findall(ddl_content)

for table_name, columns_str in tables:
    columns = column_regex.findall(columns_str)
    for column_name, column_type, nullability in columns:
        nullable_status = 'NOT NULL' if nullability and 'NOT NULL' in nullability.upper() else 'NULLABLE'
        structure.append({
            'table': table_name.strip('"'),
            'column': column_name.strip('"'),
            'type': column_type.strip(),
            'nullable': nullable_status
        })

# Запись в CSV
with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['table', 'column', 'type', 'nullable']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in structure:
        writer.writerow(row)

print(f"Структура экспортирована в {output_csv_path}")
