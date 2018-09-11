import logging
import sys
import time
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
from psycopg2.extras import execute_values

import settings


class db_connection(object):
    """
    Main class for db connection
    """

    def file_check(self, file):
        zip_filename = file['url'].split('/')[-1]
        xml_filename = zip_filename.replace('zip', 'xml')
        q = "SELECT id, status, filename, date_string FROM file_info WHERE url = %s or filename = %s"
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (file['url'], xml_filename))
            result = cur.fetchone()
            self.cnx.commit()
        except psycopg2.Error as err:
            result = None
            self.logger.error(err)
            self.cnx.rollback()
        finally:
            cur.close()
        return result

    def file_insert(self, file, xml_filename):
        q = "INSERT INTO file_info (filename, filesize, url, date_string, status) \
            VALUES (%s, %s, %s, %s, %s) RETURNING id"
        try:
            start_time = time.time()
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (xml_filename, file['size'], file['url'], file['date_string'], 'new'))
            last_row = cur.fetchone()
            self.cnx.commit()
            cur.close()
            self.logger.debug('Inserted file_info in database [%s sec]', time.time() - start_time)
            return last_row['id']
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
        return None

    def file_update_status(self, id, status):
        q = "UPDATE file_info SET status = %s, modified = now() WHERE id = %s RETURNING id, status"
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (status, id))
            rowcount = cur.rowcount
            self.cnx.commit()
            self.logger.debug('Updated status for file_info in database')
            cur.close()
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            cur.close()
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
        q = 'INSERT INTO {0} ({1}) values %s'.format(table_name, columns)
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            execute_values(cur, q, values)
            rowcount = self.cur.rowcount
            # cur.execute(q)
            # self.cnx.commit()
            cur.close()
            self.logger.debug('Inserted %s rows in table %s [%s sec]', rowcount, table_name, time.time() - start_time)
            return rowcount
        except psycopg2.Error as err:
            self.logger.error('Insert failed for table_name %s', table_name)
            self.logger.error(err)
            self.cnx.rollback()
            cur.close()
        return None

    def insert_dict(self, d, table_name):
        if d is None or table_name is None:
            logging.error('INSERT ERROR: Missing dict or table_name')
            return None
        keys = d.keys()
        columns = ', '.join(keys)
        values = ', '.join(['%({})s'.format(k) for k in keys])
        start_time = time.time()
        q = 'INSERT INTO {0} ({1}) values ({2})'.format(table_name, columns, values)
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            q = cur.mogrify(q, d)
            cur.execute(q)
            # self.cnx.commit()
            cur.close()
            self.logger.debug('Inserted row in table %s [%s sec]', table_name, time.time() - start_time)
        except psycopg2.Error as err:
            self.logger.error('Insert failed for table_name %s', table_name)
            self.logger.error(err)
            self.cnx.rollback()
            cur.close()
        return None


class Db_applications(db_connection):
    """
    Application handler
    """

    __config = settings.config

    def __init__(self):
        super().__init__
        self.logger = logging.getLogger(__name__)
        try:
            self.cnx = psycopg2.connect(**self.__config)
            # self.cur = self.get_cursor()
            self.cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.cur.execute("SET SEARCH_PATH = %s" % 'patent_app_v2')
            self.cnx.commit()
            self.cur.close()
            # self.logger.info('Connected to database')
        except psycopg2.Error as err:
            self.logger.error(err)
            sys.exit()

    def app_id_get(self, app_id, file_id):
        q = "SELECT app.id, app.date, app.filename, fi.status FROM application app \
            JOIN file_info fi ON fi.filename = app.filename WHERE app_id = %s"
        cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, (app_id,))
        result = cur.fetchone()
        self.cnx.commit()
        cur.close()
        return result

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

    def delete_application(self, app_id, table):
        if app_id is None or table is None:
            logging.error('DELETE ERROR: Missing app_id or table')
            return None
        q = 'DELETE FROM %s WHERE app_id = %s'
        start_time = time.time()
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            q = cur.mogrify(q, (AsIs(table), app_id))
            cur.execute(q)
            rowcount = cur.rowcount
            # self.cnx.commit()
            self.logger.debug(
                'Deleted %s rows from table %s for app_id %s [%s sec]',
                rowcount, table, app_id, time.time() - start_time)
            cur.close()
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
            cur.close()
        return None


class Db_grants(db_connection):
    """
    Database handler
    """

    __config = settings.config

    def __init__(self):
        super().__init__
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

    def patent_id_get(self, patent_id, file_id):
        q = "SELECT pat.id, pat.date, pat.filename, fi.status FROM patent pat \
            FULL JOIN file_info fi ON fi.filename = pat.filename WHERE pat.id = %s"
        cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, (patent_id,))
        result = cur.fetchone()
        self.cnx.commit()
        cur.close()
        return result

    def case_file_update_status(self, serial_number, status):
        if serial_number is None or status is None:
            logging.error('UPDATE ERROR: Missing serial_number or status')
            return None
        q = "UPDATE trademark_app_case_files SET status = %s, modified = now() WHERE serial_number = %s RETURNING id"
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (status, serial_number))
            rowcount = cur.rowcount
            self.cnx.commit()
            cur.close()
            self.logger.debug('Updated status for case_files in database')
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            cur.close()
        return None

    def delete_patent(self, patent_id, table):
        if patent_id is None or table is None:
            logging.error('DELETE ERROR: Missing patent_id or table')
            return None
        if table in ['patent']:
            q = 'DELETE FROM %s WHERE id = %s'
        else:
            q = 'DELETE FROM %s WHERE patent_id = %s'
        start_time = time.time()
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            q = cur.mogrify(q, (AsIs(table), patent_id))
            cur.execute(q)
            rowcount = cur.rowcount
            # self.cnx.commit()
            self.logger.debug(
                'Deleted %s rows from table %s for patent_id %s [%s sec]',
                rowcount, table, patent_id, time.time() - start_time)
            cur.close()
            return rowcount
        except psycopg2.Error as err:
            self.logger.error(err)
            self.cnx.rollback()
            cur.close()
        return None
