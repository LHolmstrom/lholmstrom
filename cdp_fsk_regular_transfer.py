from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from konnektu.connectors.postgresql_connectors import read_all_fsk_conn as source_conn, shershukov_core_fsk_v2_conn as dest_conn
from konnektu.connectors.postgresql_connectors import analytics_conn as log_conn

#vars
save_log = False # запись лога в SQL
max_active_dag = 1 # количество параллельных DAG
chunk_lim = 16384*4  # лимит чанка для dataframe
dict_path = f'/mnt/s3data/files/fsk/cookies_dict/'
source_query_path = "fsk"
source_query_file = "cdp_fsk_regular_transfer.sql"
cookies_query_file = "cdp_fsk_new_cookies.sql"
match_query_file = "cdp_fsk_new_match.sql"
dest_table = "public.CDPEvent"
match_types = {
    '00000000-0000-0000-0000-000000000000': 4,  # 'crm' - из KnkPixelCookieToSiteUser
    '86f2cfad-df56-446a-a4d3-bd7c39ddd301': 4,  # 'crm',
    '1c854f35-4ab5-41f1-bd89-0ce71540928a': 6,  # 'fbt',
    '35f5d6b0-5bec-4867-8d7f-b32ae044d32d': 6,  # 'fbt',
    '979aeb84-6392-4180-ad38-1fa34a0feed9': 7,  # 'cid',
    'a9237df0-be26-44cd-bc4a-d45985456a7f': 8,  # 'yid',
    '62582c94-8a9c-4996-8d2c-01b5d3f759a1': 9,  # 'cmg',
    '54a8f107-ccdb-4dbc-9472-c619f4cf920e': 10,  # 'scb',
    'e6e50761-5997-4fb4-9baa-ddaba8801042': 10,  # 'scb',
    'a8fa292a-d6c0-423f-a4f2-9a5f91de21b3': 10,  # 'scb',
    'c2d646a6-72ad-4f98-ac8d-7a43eefa339f': 11,  # 'mob',
}

local_tz = pendulum.timezone('Europe/Moscow')
default_args = {
    'owner': 'prokhorov_a',
    'start_date': datetime(2022, 10, 24, tzinfo=local_tz),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
start_date = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
end_date = "{{ data_interval_end.strftime('%Y-%m-%d') }}"


def on_success_callback(context):
    date_ = context.get("data_interval_end")
    dag_id_ = context.get("task_instance").dag_id
    url = 'http://172.27.15.15:8080/tree?dag_id='
    send_message = TelegramOperator(
        task_id='send_message_telegram_success',
        telegram_conn_id='konnektu_logs',
        text=f'😊 #SUCCESS {date_}\n\n{dag_id_}\n\nGo to DAG {url}{dag_id_}',
        dag=dag
    )
    return send_message.execute(context=context)


def on_failure_callback(context):
    date_ = context.get("data_interval_end")
    dag_id_ = context.get("task_instance").dag_id
    url = 'http://172.27.15.15:8080/tree?dag_id='
    send_message = TelegramOperator(
        task_id='send_message_telegram_failure',
        telegram_conn_id='konnektu_logs',
        text=f'👿 #FAILED {date_}\n\n{dag_id_}\n\nGo to DAG {url}{dag_id_}',
        dag=dag
    )
    return send_message.execute(context=context)


with DAG(
        'cdp_fsk_regular_transfer',
        description='Ежедневно собираем данные CDP',
        default_args=default_args,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        schedule_interval='5 5 * * *',
        max_active_runs=max_active_dag,
        catchup=True,
        tags=['shershukov_a', 'fsk', 'cdp']
) as dag:

    def logging(step: str, start_date: str, params=None):
        from sqlalchemy import create_engine

        log_engine = create_engine(log_conn, execution_options={"isolation_level": "AUTOCOMMIT"})
        with log_engine.connect() as conn:
            conn.execute(f"""
                insert into cron_log ("CronName", "CreatedOn", "ExecutionDate", "Params")
                values ('{step}',
                    '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                    '{start_date}',
                    '{str(params)}')
            """)
        log_engine.dispose()

    def extract_schema(dest_table: str):
        if '.' in dest_table:
            return dest_table.split('.')
        else:
            return ('public.' + dest_table).split('.')

    def get_cdp_pixel_id() -> dict:
        from sqlalchemy import create_engine

        d = {}
        qry = f"""select "WhiteId"::text, "Id" from "Pixel";"""
        dest_engine = create_engine(dest_conn)
        with dest_engine.connect().execution_options(stream_results=True) as conn:
            for row in conn.execute(qry):
                d[row[0]] = row[1]
        dest_engine.dispose()
        return d

    def get_cdp_identifier_id(values: tuple) -> dict:
        from sqlalchemy import create_engine

        if len(values) == 0:
            return {}
        d = {}
        qry = f"""select "Value", "Id" from "CDPIdentifier" where "Value" in (%s);"""
        qry = qry % ','.join(['%s'] * len(values))

        dest_engine = create_engine(dest_conn)
        with dest_engine.connect().execution_options(stream_results=True) as conn:
            for row in conn.execute(qry, values):
                d[row[0]] = row[1]
        dest_engine.dispose()
        return d

    def get_cdp_user_id(end_date: str) -> dict:
        from sqlalchemy import create_engine

        d = {}
        qry = f"""
            select cim."IdentifierId", u."Id" as "UserId"
            from "CDPIdentifierMatch" cim
                inner join "User" u on cim."Value" = u."WhiteId"::text
            where cim."RuleId" = 4
                and cim."CreatedOn" < '{end_date}'
        """
        dest_engine = create_engine(dest_conn)
        with dest_engine.connect().execution_options(stream_results=True) as conn:
            for row in conn.execute(qry):
                d[row[0]] = row[1]
        dest_engine.dispose()
        return d

    def get_cdp_identifier_match_id(rule: int, values: tuple) -> dict:
        from sqlalchemy import create_engine

        if len(values) == 0:
            return {}
        d = {}
        qry = f"""
            select "Value", {rule} as "RuleId"
            from "CDPIdentifierMatch"
            where "RuleId" = {rule} and "Value" in (%s)
        """
        qry = qry % ','.join(['%s'] * len(values))

        dest_engine = create_engine(dest_conn)
        with dest_engine.connect().execution_options(stream_results=True) as conn:
            for row in conn.execute(qry, values):
                d[row[0]] = row[1]
        dest_engine.dispose()
        return d

    def get_nextval_seq(seq: str) -> int:
        from sqlalchemy import create_engine

        dest_engine = create_engine(dest_conn)
        with dest_engine.connect() as conn:
            result = conn.execute(f"""select nextval('"{seq}"')""").fetchone()[0]
        dest_engine.dispose()
        return result

    def restart_seq(seq: str, restart_value: int):
        from sqlalchemy import create_engine

        dest_engine = create_engine(dest_conn)
        with dest_engine.connect() as conn:
            conn.execute(f"""alter sequence "{seq}" restart with {restart_value}""")
        dest_engine.dispose()

    def get_nextval_seq_dict(seq: str, values: tuple, dct={}) -> dict:
        from sqlalchemy import create_engine

        if len(values) == 0:
            return dct
        dest_engine = create_engine(dest_conn)
        with dest_engine.connect() as conn:
            for value in values:
                dct[value] = conn.execute(f"""select nextval('"{seq}"')""").fetchone()[0]
                # result = (x for x in conn.execute(f"""select nextval('"{seq}"')""").fetchall()[0])
        dest_engine.dispose()
        return dct

    def save_dict_to_csv(dct: dict, fname: str):
        with open(fname, 'w') as f:
            for key in dct.keys():
                f.write("%s, %s\n" % (key, dct[key]))

    def load_dict_from_csv(fname: str) -> dict:
        import csv

        reader = csv.reader(open(fname, 'r'))
        d = {}
        for row in reader:
            k, v = row
            d[k] = int(v)
        return d

    def check_file(fname: str):
        import os
        return os.path.isfile(fname)

    def remove_file(fname: str):
        import os
        if os.path.isfile(fname):
            os.remove(fname)

    def change_match(lst: object, cookies_dict: dict, match_types: dict) -> list:
        lst = list(lst)
        lst[0] = cookies_dict[lst[0]]
        lst[2] = match_types[lst[2]]
        return lst

    def get_cookies_df(start_date: str, end_date: str):
        import pandas as pd
        # from sqlalchemy import create_engine
        from konnektu.utils.sqlalchemy.postgresql.facade import sql_file_reader

        query = sql_file_reader(source_query_path, cookies_query_file, start_date=start_date, end_date=end_date)
        query = query % ','.join("'" + x + "'" for x in match_types.keys())

        # source_engine = create_engine(source_conn)
        # with source_engine.connect().execution_options(stream_results=True) as conn:
        #     result = pd.read_sql(query, conn)
        # source_engine.dispose()
        result = pd.read_sql(query, source_conn)
        if save_log:
            logging('get_cookies_df', start_date, result.shape)
        return result

    def get_match_df(start_date: str, end_date: str):
        import pandas as pd
        # from sqlalchemy import create_engine
        from konnektu.utils.sqlalchemy.postgresql.facade import sql_file_reader

        query = sql_file_reader(source_query_path, match_query_file, start_date=start_date, end_date=end_date)
        query = query % ','.join("'" + x + "'" for x in match_types.keys())

        # source_engine = create_engine(source_conn)
        # with source_engine.connect().execution_options(stream_results=True) as conn:
        #     result = pd.read_sql(query, conn)
        # source_engine.dispose()
        result = pd.read_sql(query, source_conn)
        if save_log:
            logging('get_match_df', start_date, result.shape)
        return result

    def insert_cookies_df(start_date: str, end_date: str):
        from sqlalchemy import create_engine

        # Логирование
        if save_log:
            logging('Start insert_cookies_df', start_date, start_date)
        # Загружаем справочник
        cksdf = get_cookies_df(start_date, end_date)
        # Выход при отсутствии данных
        if len(cksdf) == 0:
            return None
        # Формируем словарь
        cks = get_cdp_identifier_id(cksdf.Value)
        if save_log:
            logging('Формируем словарь (cks)', start_date, len(cks))
        # Обновляем IdentifierId
        # cksdf['IdentifierId'] = cksdf['Value'].apply(lambda x: str(x)).map(cks)
        cksdf['IdentifierId'] = cksdf['Value'].map(cks)
        if save_log:
            logging('Обновляем IdentifierId (cksdf)', start_date, cksdf.shape)
        # Открываем подключение для вставки
        dest_engine = create_engine(dest_conn, execution_options={"isolation_level": "AUTOCOMMIT"})
        # Проверки и вставки
        if cksdf['IdentifierId'].isna().any():
            cksdf.loc[cksdf['IdentifierId'].isna()][['Value', 'CreatedOn', 'ModifiedOn']].to_sql(
                'CDPIdentifier',
                dest_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            cks = get_cdp_identifier_id(cksdf.Value)
            # cksdf['IdentifierId'] = cksdf['Value'].apply(lambda x: str(x)).map(cks)
            cksdf['IdentifierId'] = cksdf['Value'].map(cks)
            if save_log:
                logging('Вставка CDPIdentifier', start_date, len(cks))
        # fpc
        # cksdf['Rule1'] = cksdf['Value'].apply(lambda x: str(x)).map(get_cdp_identifier_match_id(1, cksdf.Value))
        cksdf['Rule1'] = cksdf['Value'].map(get_cdp_identifier_match_id(1, cksdf.Value))
        if cksdf['Rule1'].isna().any():
            cksdf['RuleId'] = 1
            cksdf.loc[cksdf['Rule1'].isna()][['IdentifierId', 'Value', 'RuleId', 'CreatedOn', 'ModifiedOn']].to_sql(
                'CDPIdentifierMatch',
                dest_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            if save_log:
                logging('Вставка fpc', start_date, 'fpc')
        # tpc
        # cksdf['Rule2'] = cksdf['Value'].apply(lambda x: str(x)).map(get_cdp_identifier_match_id(2, cksdf.Value))
        cksdf['Rule2'] = cksdf['Value'].map(get_cdp_identifier_match_id(2, cksdf.Value))
        if cksdf['Rule2'].isna().any():
            cksdf['RuleId'] = 2
            cksdf.loc[cksdf['Rule2'].isna()][['IdentifierId', 'Value', 'RuleId', 'CreatedOn', 'ModifiedOn']].to_sql(
                'CDPIdentifierMatch',
                dest_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            if save_log:
                logging('Вставка tpc', start_date, 'tpc')
        dest_engine.dispose()
        if save_log:
            logging('insert_cookies_df', start_date, cksdf.shape)
        # Сохраняем словарь
        save_dict_to_csv(cks, dict_path + f'cookies_dict_{start_date}.csv')
        if save_log:
            logging('Сохраняем словарь (cks)', start_date, len(cks))

    insert_cookies_df_ = PythonOperator(
        task_id=f'insert_cookies_df',
        python_callable=insert_cookies_df,
        op_args=[start_date, end_date],
    )


    def insert_match_df(start_date: str, end_date: str):
        from sqlalchemy import create_engine

        # Логирование
        if save_log:
            logging('Start insert_match_df', start_date, start_date)
        # Загружаем справочник
        mtchdf = get_match_df(start_date, end_date)
        # Выход при отсутствии данных
        if len(mtchdf) == 0:
            return None
        # Загружаем словарь
        # cks = get_cdp_identifier_id(mtchdf.Identifier)
        # Проверка на наличие словаря
        if check_file(dict_path + f'cookies_dict_{start_date}.csv'):
            cks = load_dict_from_csv(dict_path + f'cookies_dict_{start_date}.csv')
            if save_log:
                logging('Загружаем словарь (cks)', start_date, len(cks))
        else:
            return None
        # Обновляем IdentifierId
        # mtchdf['IdentifierId'] = mtchdf['Identifier'].apply(lambda x: str(x)).map(cks)
        mtchdf['IdentifierId'] = mtchdf['Identifier'].map(cks)
        if save_log:
            logging('Обновляем IdentifierId (mtchdf)', start_date, mtchdf.shape)
        # Обновляем RuleId
        # mtchdf['RuleId'] = mtchdf['TypeId'].apply(lambda x: str(x)).map(match_types)
        mtchdf['RuleId'] = mtchdf['TypeId'].map(match_types)
        # Формируем обновленный фрейм
        # mtchdf = mtchdf[['IdentifierId', 'RuleId', 'Value', 'CreatedOn', 'ModifiedOn']].groupby(
        #     ['IdentifierId', 'RuleId', 'Value']).min().reset_index()
        mtchdf = mtchdf[['IdentifierId', 'RuleId', 'Value', 'CreatedOn', 'ModifiedOn']].groupby(
            ['RuleId', 'Value']).min().reset_index()
        if save_log:
            logging('Формируем обновленный фрейм (mtchdf)', start_date, mtchdf.shape)
        # Проверяем Match
        for tp in mtchdf['RuleId'].unique():
            mtchdf.loc[mtchdf['RuleId'] == tp, 'Check'] = mtchdf['Value'].map(get_cdp_identifier_match_id(tp, mtchdf.Value))
        if save_log:
            logging('Проверяем Match (mtchdf)', start_date, mtchdf.shape)
        # Открываем подключение для вставки
        dest_engine = create_engine(dest_conn, execution_options={"isolation_level": "AUTOCOMMIT"})
        # Проверки и вставки
        if mtchdf['Check'].isna().any():
            mtchdf.loc[mtchdf['Check'].isna()][['IdentifierId', 'RuleId', 'Value', 'CreatedOn', 'ModifiedOn']].to_sql(
                'CDPIdentifierMatch',
                dest_engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
            if save_log:
                logging('Проверки и вставки (mtchdf)', start_date, mtchdf.shape)
        dest_engine.dispose()
        if save_log:
            logging('insert_match_df', start_date, mtchdf.shape)

    insert_match_df_ = PythonOperator(
        task_id=f'insert_match_df',
        python_callable=insert_match_df,
        op_args=[start_date, end_date],
    )


    def delete_partition(start_date: str, end_date: str):
        from sqlalchemy import create_engine

        dest_engine = create_engine(dest_conn)
        dest_table_schema = extract_schema(dest_table)
        with dest_engine.connect() as conn:
            conn.execute(f"""delete from {dest_table_schema[0]}."{dest_table_schema[1]}" 
                            where "CreatedOn" >= '{start_date}' and "CreatedOn" < '{end_date}';""")
        dest_engine.dispose()
        if save_log:
            logging('delete_partition', start_date, start_date)

    # delete_partition_ = PythonOperator(
    #     task_id=f'partition_cdp_delete_from_{dest_table}',
    #     python_callable=delete_partition,
    #     op_args=[start_date, end_date],
    # )


    def insert_cdp_event(start_date: str, end_date: str, chunk=chunk_lim):
        import numpy as np
        import pandas as pd
        from sqlalchemy import create_engine
        from urllib.parse import urlparse
        from konnektu.utils.sqlalchemy.postgresql.facade import sql_file_reader

        if chunk > chunk_lim:
            chunk = chunk_lim
            print('Максимальный размер чанка ограничен на', chunk)

        # Удаление предыдущих данных
        delete_partition(start_date, end_date)
        # Счетчик чанков
        cnt = 0
        # Колонки для выгрузки
        clmns = ['Id',
                 'PixelId', 'IdentifierId', 'UserId',
                 'CreatedOn', 'ClientCreatedOn', 'SessionStart',
                 'Name', 'SessionId', 'Referer',
                 'EntityName', 'EntityId', 'EntityHref', 'EntityCls',
                 'IsHttps', 'Hostname', 'Path', 'QueryString',
                 'UtmCampaign', 'UtmSource', 'UtmMedium', 'UtmTerm', 'UtmContent',
                 'WindowWidth', 'WindowHeight', 'ScreenWidth', 'ScreenHeight',
                 'Scale', 'Angle', 'ScrollX', 'ScrollY',
                 'UserAgent',
                 'BrowserName', 'BrowserMajor', 'BrowserMinor', 'BrowserPatch',
                 'DeviceBrand', 'DeviceFamily', 'DeviceModel', 'DeviceIsSpider',
                 'OsFamily', 'OsMajor', 'OsMinor', 'OsPatch', 'OsPatchMinor']
        # Создаем словарь для балльной механики
        evnt = {}
        # Удаляем сохраненный файл словаря
        remove_file(dict_path + f'events_dict_{start_date}.csv')
        # Загружаем словарь pixel
        pxl = get_cdp_pixel_id()
        if save_log:
            logging('Загружаем словарь pixel', start_date, len(pxl))
        # Загружаем словарь cookies
        # cks = get_cdp_identifier_id(mtchdf.Identifier)
        if check_file(dict_path + f'cookies_dict_{start_date}.csv'):
            cks = load_dict_from_csv(dict_path + f'cookies_dict_{start_date}.csv')
            if save_log:
                logging('Загружаем словарь cookies', start_date, len(cks))
        else:
            return None
        # Загружаем словарь users
        usr = get_cdp_user_id(end_date)
        if save_log:
            logging('Загружаем словарь users', start_date, len(usr))
        # Обработка
        dest_table_schema = extract_schema(dest_table)
        source_query = sql_file_reader(source_query_path, source_query_file, start_date=start_date, end_date=end_date)
        source_engine = create_engine(source_conn)
        with source_engine.connect().execution_options(stream_results=True) as conn:
            for chunk_dataframe in pd.read_sql(source_query, conn, chunksize=chunk):
                cnt = cnt + 1 #Счетчик чанков
                if save_log:
                    logging('Загрузили чанк', start_date, cnt)
                # # Установка Id
                # evnt = get_nextval_seq_dict('CDPEvent_Id_seq', chunk_dataframe.WhiteId, evnt)
                # if save_log:
                #     logging('Сбор словаря Id', start_date, cnt)
                # chunk_dataframe['Id'] = chunk_dataframe['WhiteId'].apply(lambda x: str(x)).map(evnt)
                # if save_log:
                #     logging('Установка Id', start_date, cnt)
                # Установка и запись Id
                chunklen = len(chunk_dataframe)
                nextval = get_nextval_seq('CDPEvent_Id_seq')
                restart_seq('CDPEvent_Id_seq', int(nextval + chunklen + 1))
                chunk_dataframe['Id'] = np.arange(nextval, nextval + chunklen)
                # Запись словаря
                chunk_dataframe[['WhiteId', 'Id']].to_csv(dict_path + f'events_dict_{start_date}.csv',
                                                          mode='a', header=False, index=False)
                if save_log:
                    logging('Сохраняем словарь (evnt)', start_date, cnt)
                # Установка PixelId
                # chunk_dataframe['PixelId'] = chunk_dataframe['Pixel'].apply(lambda x: str(x)).map(pxl)
                chunk_dataframe['PixelId'] = chunk_dataframe['Pixel'].map(pxl)
                # Установка CDPIdentifierId
                # chunk_dataframe['IdentifierId'] = chunk_dataframe['Identifier'].apply(lambda x: str(x)).map(cks)
                chunk_dataframe['IdentifierId'] = chunk_dataframe['Identifier'].map(cks)
                # Установка UserId
                # chunk_dataframe['UserId'] = chunk_dataframe['IdentifierId'].apply(lambda x: str(x)).map(usr)
                chunk_dataframe['UserId'] = chunk_dataframe['IdentifierId'].map(usr)
                if save_log:
                    logging('Установка PixelId, IdentifierId, UserId', start_date, cnt)
                # Обработка urrlib
                chunk_dataframe['Hostname'] = chunk_dataframe['Uri'].apply(lambda x: urlparse(x).netloc)
                chunk_dataframe['Path'] = chunk_dataframe['Uri'].apply(lambda x: urlparse(x).path)
                chunk_dataframe['QueryString'] = chunk_dataframe['Uri'].apply(lambda x: urlparse(x).query)
                numbers = [x for x in range(0, 101)]
                chunk_dataframe.loc[~chunk_dataframe['Angle'].isin(numbers),['Angle']] = 0
                if save_log:
                    logging('Обработка urrlib', start_date, cnt)
                # Обработка NaN
                chunk_dataframe[[
                    'WindowWidth', 'WindowHeight', 'ScreenWidth', 'ScreenHeight',
                    'Scale', 'Angle', 'ScrollX', 'ScrollY'
                ]] = chunk_dataframe[[
                    'WindowWidth', 'WindowHeight', 'ScreenWidth', 'ScreenHeight',
                    'Scale', 'Angle', 'ScrollX', 'ScrollY'
                ]].fillna(0)
                if save_log:
                    logging('Обработали чанк', start_date, cnt)
                chunk_dataframe[clmns].to_sql(
                    dest_table_schema[1],
                    dest_conn,
                    schema=dest_table_schema[0],
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                if save_log:
                    logging('Записали чанк', start_date, cnt)
        source_engine.dispose()
        if save_log:
            logging('insert_cdp_event', start_date, len(evnt))
        # # Сохраняем словарь
        # save_dict_to_csv(evnt, dict_path + f'events_dict_{start_date}.csv')
        # if save_log:
        #     logging('Сохраняем словарь (evnt)', start_date, len(evnt))


    insert_cdp_event_ = PythonOperator(
        task_id=f'insert_cdp_event_to_{dest_table}',
        python_callable=insert_cdp_event,
        op_args=[start_date, end_date],
    )


    insert_cookies_df_ >> insert_match_df_ >> insert_cdp_event_