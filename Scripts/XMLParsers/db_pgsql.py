import logging
import sys
import time
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
from psycopg2.extras import execute_values

import settings


class Db_applications(object):
    """
    Database handler
    """

    __config = settings.config

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        try:
            self.cnx = psycopg2.connect(**self.__config)
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute("SET SEARCH_PATH = %s" % 'patent_app_v2')
            self.cnx.commit()
            # self.logger.info('Connected to database')
        except psycopg2.Error as err:
            self.logger.error(err)

    def file_check(self, file):
        zip_filename = file['url'].split('/')[-1]
        xml_filename = zip_filename.replace('zip', 'xml')
        q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
        self.cur.execute(q, (file['url'], xml_filename))
        return self.cur.fetchone()

    def file_insert(self, file, xml_filename):
        q = "INSERT INTO file_info (filename, filesize, url, date_string) VALUES (%s, %s, %s, %s) RETURNING id"
        try:
            start_time = time.time()
            self.cur.execute(q, (xml_filename, file['size'], file['url'], file['date_string']))
            last_row = self.cur.fetchone()
            self.cnx.commit()
            self.logger.debug('Inserted file_info in database [%s sec]', time.time() - start_time)
            return last_row['id']
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None

    def file_update_status(self, id, status):
        q = "UPDATE file_info SET status = %s, modified = now() WHERE id = %s RETURNING id, status"
        try:
            self.cur.execute(q, (status, id))
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.logger.debug('Updated status for file_info in database')
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
        return None

    def app_id_get(self, app_id, file_id):
        q = "SELECT app.id, app.date, app.filename, fi.status FROM application app \
            JOIN file_info fi ON fi.filename = app.filename WHERE app_id = %s"
        self.cur.execute(q, (app_id,))
        return self.cur.fetchone()

    def case_file_update_status(self, serial_number, status):
        if serial_number is None or status is None:
            logging.error('UPDATE ERROR: Missing serial_number or status')
            return None
        q = "UPDATE trademark_app_case_files SET status = %s, modified = now() WHERE serial_number = %s RETURNING id"
        try:
            self.cur.execute(q, (status, serial_number))
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.logger.debug('Updated status for case_files in database')
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
        return None

    def delete_serial(self, serial_number, table):
        if serial_number is None or table is None:
            logging.error('DELETE ERROR: Missing serial_number or table')
            return None
        q = 'DELETE FROM %s WHERE serial_number = %s'
        start_time = time.time()
        try:
            q = self.cur.mogrify(q, (AsIs(table), serial_number))
            self.cur.execute(q)
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.logger.warning('Deleted serial_number %s from table %s [%s sec]', serial_number, table, time.time() - start_time)
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None

    def insert_listdict(self, lst, table):
        if len(lst) == 0 :
            return None
        keys = lst[0].keys()
        columns = ', '.join(keys)
        values = []
        for d in lst:
            values.append(list(d.values()))
        start_time = time.time()
        q = 'INSERT INTO {0} ({1}) values %s RETURNING id'.format(table, columns)
        try:
            execute_values(self.cur, q, values)
            rowcount = self.cur.rowcount
            # self.cur.execute(q)
            self.cnx.commit()
            self.logger.debug('Inserted %s rows in table %s [%s sec]', rowcount, table, time.time() - start_time)
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None

    def insert_dict(self, d, table):
        if d is None or table is None:
            logging.error('INSERT ERROR: Missing dict or table')
            return None
        keys = d.keys()
        columns = ', '.join(keys)
        values = ', '.join(['%({})s'.format(k) for k in keys])
        start_time = time.time()
        q = 'INSERT INTO {0} ({1}) values ({2}) RETURNING id'.format(table, columns, values)
        try:
            q = self.cur.mogrify(q, d)
            self.cur.execute(q)
            self.cnx.commit()
            last_row = self.cur.fetchone()
            self.logger.debug('Inserted id [%s] in table %s [%s sec]', last_row['id'], table, time.time() - start_time)
            return last_row['id']
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None


class Db_grants(object):
    """
    Database handler
    """

    __config = settings.config

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        try:
            self.cnx = psycopg2.connect(**self.__config)
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute("SET SEARCH_PATH = %s" % 'patent_grants_v2')
            self.cnx.commit()
            self.cur.close()
            # self.logger.info('Connected to database')
        except psycopg2.Error as err:
            self.logger.error(err)
            sys.exit()

    def file_check(self, file):
        zip_filename = file['url'].split('/')[-1]
        xml_filename = zip_filename.replace('zip', 'xml')
        q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute(q, (file['url'], xml_filename))
            result = self.cur.fetchone()
        except psycopg2.Error as err:
            result = None
            self.logger.error(err)
            self.cnx.rollback()
        finally:
            self.cur.close()
        return result

    def file_insert(self, file, xml_filename):
        q = "INSERT INTO file_info (filename, filesize, url, date_string, status) VALUES (%s, %s, %s, %s, %s) RETURNING id"
        try:
            start_time = time.time()
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute(q, (xml_filename, file['size'], file['url'], file['date_string'], 'new'))
            last_row = self.cur.fetchone()
            self.cnx.commit()
            self.cur.close()
            self.logger.debug('Inserted file_info in database [%s sec]', time.time() - start_time)
            return last_row['id']
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None

    def file_update_status(self, id, status):
        q = "UPDATE file_info SET status = %s, modified = now() WHERE id = %s RETURNING id, status"
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute(q, (status, id))
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.logger.debug('Updated status for file_info in database')
            self.cur.close()
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cur.close()
        return None

    def patent_id_get(self, patent_id, file_id):
        q = "SELECT pat.id, pat.date, pat.filename, fi.status FROM patent pat \
            JOIN file_info fi ON fi.filename = pat.filename WHERE pat.id = %s"
        self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self.cur.execute(q, (patent_id,))
        result = self.cur.fetchone()
        self.cur.close()
        return result

    def case_file_update_status(self, serial_number, status):
        if serial_number is None or status is None:
            logging.error('UPDATE ERROR: Missing serial_number or status')
            return None
        q = "UPDATE trademark_app_case_files SET status = %s, modified = now() WHERE serial_number = %s RETURNING id"
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute(q, (status, serial_number))
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.cur.close()
            self.logger.debug('Updated status for case_files in database')
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cur.close()
        return None

    def delete_serial(self, serial_number, table):
        if serial_number is None or table is None:
            logging.error('DELETE ERROR: Missing serial_number or table')
            return None
        q = 'DELETE FROM %s WHERE serial_number = %s'
        start_time = time.time()
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            q = self.cur.mogrify(q, (AsIs(table), serial_number))
            self.cur.execute(q)
            rowcount = self.cur.rowcount
            self.cnx.commit()
            self.logger.warning('Deleted serial_number %s from table %s [%s sec]', serial_number, table, time.time() - start_time)
            self.cur.close()
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
            self.cur.close()
        return None

    def insert_listdict(self, lst, table_name):
        if len(lst) == 0:
            return None
        keys = lst[0].keys()
        columns = ', '.join(keys)
        values = []
        for d in lst:
            values.append(list(d.values()))
        start_time = time.time()
        q = 'INSERT INTO {0} ({1}) values %s RETURNING id'.format(table_name, columns)
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            execute_values(self.cur, q, values)
            rowcount = self.cur.rowcount
            # self.cur.execute(q)
            self.cnx.commit()
            self.cur.close()
            self.logger.debug('Inserted %s rows in table %s [%s sec]', rowcount, table_name, time.time() - start_time)
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
            self.cur.close()
        return None

    def insert_dict(self, d, table_name):
        if d is None or table_name is None:
            logging.error('INSERT ERROR: Missing dict or table_name')
            return None
        keys = d.keys()
        columns = ', '.join(keys)
        values = ', '.join(['%({})s'.format(k) for k in keys])
        start_time = time.time()
        q = 'INSERT INTO {0} ({1}) values ({2}) RETURNING id'.format(table_name, columns, values)
        try:
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            q = self.cur.mogrify(q, d)
            self.cur.execute(q)
            self.cnx.commit()
            last_row = self.cur.fetchone()
            self.cur.close()
            self.logger.debug('Inserted id [%s] in table %s [%s sec]', last_row['id'], table_name, time.time() - start_time)
            return last_row['id']
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
            self.cur.close()
        return None
