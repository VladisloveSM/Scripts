import cx_Oracle
import datetime
import decimal
import sys
import os

OUTPUT_DIR = r"1.DEVDEV-11446"
INPUT_FILE = r"input.txt"
VIEWS_FILE = r"views.txt"

# ---------- Подключения ----------

def get_db_connection():
    """Основная БД (тестовое подключение)"""
    dsn = cx_Oracle.makedsn("localhost", 1521, service_name="TEST")
    return cx_Oracle.connect(user="test_user", password="test_password", dsn=dsn)

def get_view_db_connection():
    """Отдельное подключение для DDL VIEW (тестовое подключение)"""
    dsn = cx_Oracle.makedsn("localhost", 1521, service_name="TEST")
    return cx_Oracle.connect(user="test_view_user", password="test_view_password", dsn=dsn)

# ---------- Вспомогательные функции ----------

def escape_sql_value(value: str) -> str:
    return value.replace("'", "''")

def read_text_from_val(val):
    if val is None:
        return None
    if hasattr(val, "read"):
        txt = val.read()
        if isinstance(txt, (bytes, bytearray)):
            return txt.decode("utf-8", errors="replace")
        return str(txt)
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    return str(val)

def is_numeric_val(val):
    return isinstance(val, (int, float, decimal.Decimal)) and not isinstance(val, bool)

def is_clob_column_type(col_type):
    try:
        if col_type == cx_Oracle.CLOB:
            return True
    except Exception:
        pass
    tname = getattr(col_type, "__name__", None) or str(col_type)
    return "CLOB" in tname.upper() or "LOB" in tname.upper()

def process_value(val, col_type, sql_lines, clob_vars, clob_alloc):
    if val is None:
        return "NULL"
    if is_clob_column_type(col_type):
        idx = clob_alloc.get("index", 1)
        if idx > len(clob_vars):
            idx = len(clob_vars)
        var_name = clob_vars[idx - 1]
        text = read_text_from_val(val)
        sql_lines.append(f"    {var_name} := '{escape_sql_value(text)}';")
        clob_alloc["index"] = idx + 1
        return var_name
    if is_numeric_val(val):
        return str(val)
    if isinstance(val, (datetime.datetime, datetime.date)):
        if isinstance(val, datetime.datetime):
            s = val.strftime("%Y-%m-%d %H:%M:%S")
            return f"TO_DATE('{s}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            s = val.strftime("%Y-%m-%d")
            return f"TO_DATE('{s}', 'YYYY-MM-DD')"
    text = read_text_from_val(val)
    return f"'{escape_sql_value(text)}'"

def parse_line(line: str):
    parts = [p.strip() for p in line.split(",") if p.strip()]
    key_values = {}
    fields = []
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            key_values[k.strip()] = v.strip()
        else:
            fields.append(p)
    return key_values, fields

def safe_fetch_one(cursor, sql, params, table_name, keys):
    cursor.execute(sql, params)
    results = cursor.fetchall()
    if len(results) == 0:
        print(f"[⚠️ WARNING] В таблице {table_name} не найдена запись по ключу: {keys}")
        return None
    elif len(results) > 1:
        print(f"[❌ ERROR] В таблице {table_name} найдено несколько ({len(results)}) записей по ключу: {keys}. Пропускаем.")
        return None
    return results[0]

# ---------- Генерация SQL по таблицам ----------

def process_table(conn, table_name, data_lines, file_index):
    cursor = conn.cursor()
    sql_lines = []
    sql_lines.append("DECLARE")
    sql_lines.append("    v_clob1 CLOB;")
    sql_lines.append("    v_clob2 CLOB;")
    sql_lines.append("    v_clob3 CLOB;")
    sql_lines.append("BEGIN")

    clob_vars = ["v_clob1", "v_clob2", "v_clob3"]

    for line_num, line in enumerate(data_lines, start=1):
        keys, fields = parse_line(line)
        if not keys:
            print(f"[⚠️ WARNING] В таблице {table_name}, строка {line_num}: не указаны ключи для выборки.")
            continue

        where_cond = " AND ".join(f"{k} = '{escape_sql_value(v)}'" for k, v in keys.items())
        clob_alloc = {"index": 1}

        if not fields:
            sql = f"SELECT * FROM {table_name} WHERE " + " AND ".join(f"{k} = :{k}" for k in keys)
            result = safe_fetch_one(cursor, sql, keys, table_name, keys)
            if not result:
                continue

            columns = [d[0] for d in cursor.description]
            types = [d[1] for d in cursor.description]

            values_sql = [
                process_value(val, col_type, sql_lines, clob_vars, clob_alloc)
                for val, col_type in zip(result, types)
            ]

            sql_lines.append(f"    DELETE FROM {table_name} WHERE {where_cond};")
            sql_lines.append(f"    INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values_sql)});")
            sql_lines.append("")

        else:
            sql = f"SELECT {', '.join(fields)} FROM {table_name} WHERE " + " AND ".join(f"{k} = :{k}" for k in keys)
            result = safe_fetch_one(cursor, sql, keys, table_name, keys)
            if not result:
                continue

            types = [d[1] for d in cursor.description]
            for col, val, col_type in zip(fields, result, types):
                expr = process_value(val, col_type, sql_lines, clob_vars, clob_alloc)
                sql_lines.append(f"    UPDATE {table_name} SET {col} = {expr} WHERE {where_cond};")
            sql_lines.append("")

    sql_lines.append("    COMMIT;")
    sql_lines.append("END;")
    sql_lines.append("/")  # ✅ завершение блока

    output_file = os.path.join(OUTPUT_DIR, f"{file_index}.{table_name}.sql")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_lines))

    print(f"[✅] Файл {output_file} успешно создан.\n")

# ---------- Обработка VIEW ----------

def process_views_file():
    if not os.path.exists(VIEWS_FILE):
        print("[ℹ️] Файл views.txt не найден — пропускаем обработку VIEW.")
        return

    with open(VIEWS_FILE, "r", encoding="utf-8") as f:
        views = [line.strip() for line in f if line.strip()]

    if not views:
        print("[ℹ️] Файл views.txt пуст.")
        return

    conn = get_view_db_connection()
    cursor = conn.cursor()

    for i, view_name in enumerate(views, start=1):
        try:
            cursor.execute("SELECT DBMS_METADATA.GET_DDL('VIEW', :v) FROM DUAL", {"v": view_name.upper()})
            ddl = cursor.fetchone()
            
            if not ddl or not ddl[0]:
                print(f"[⚠️ WARNING] Не удалось получить DDL для {view_name}")
                continue

            text = ddl[0].read() if hasattr(ddl[0], "read") else str(ddl[0])
            output_file = f"{i}.{view_name}.vw"
            with open(output_file, "w", encoding="utf-8") as out:
                out.write(text.strip() + "\n/\n")

            print(f"[✅] VIEW {view_name} сохранён в {output_file}")
        except Exception as e:
            print(f"[❌ ERROR] Ошибка при обработке {view_name}: {e}")

    conn.close()

# ---------- MAIN ----------

def main():
    # 1️⃣ обработка input.txt
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"[⚠️ WARNING] Файл {INPUT_FILE} не найден — пропускаем обработку таблиц.")
        lines = []

    if lines:
        conn = get_db_connection()
        tables = {}
        current_table = None
        for line in lines:
            if "=" not in line and "," not in line:
                current_table = line.strip()
                tables[current_table] = []
            elif current_table:
                tables[current_table].append(line.strip())

        for index, (table, data_lines) in enumerate(tables.items(), start=1):
            print(f"\n--- Обработка таблицы {table} ---")
            process_table(conn, table, data_lines, index)

        conn.close()
        print("\n✅ Работа с таблицами завершена.")
    else:
        print("[ℹ️] Файл input.txt пуст или отсутствует данные для таблиц.")

    # 2️⃣ обработка views.txt
    print("\n--- Обработка VIEW ---")
    process_views_file()
    print("\n✅ Работа завершена.")

if __name__ == "__main__":
    main()
