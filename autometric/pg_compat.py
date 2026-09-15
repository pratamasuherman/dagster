"""
Shim buat menjaga API psycopg2.extras.execute_values() dipakai apa adanya
setelah migrasi ke psycopg (v3) -- alasan migrasi: WDAC kantor blokir DLL
psycopg2-binary/psycopg-binary yang tidak "Enterprise signed" (lihat
resources.py). psycopg3 tidak punya execute_values bawaan.
"""

from typing import Sequence


def execute_values(cur, sql: str, argslist: Sequence[Sequence], page_size: int = 100) -> None:
    """
    Drop-in pengganti psycopg2.extras.execute_values untuk psycopg3.
    sql harus mengandung literal 'VALUES %s' persis sekali.
    """
    argslist = list(argslist)
    if not argslist:
        return

    values_placeholder = "VALUES %s"
    if values_placeholder not in sql:
        raise ValueError("sql harus mengandung 'VALUES %s'")

    for start in range(0, len(argslist), page_size):
        batch = argslist[start:start + page_size]
        row_sql = "(" + ",".join(["%s"] * len(batch[0])) + ")"
        query = sql.replace(values_placeholder, "VALUES " + ",".join([row_sql] * len(batch)))
        flat_params = [v for row in batch for v in row]
        cur.execute(query, flat_params)
