import argparse
import concurrent.futures as cf
import logging
import logging.config
import os
import random
import re
import requests
import string
import sys
import time
import zipfile
from lxml import etree, html

from db_pgsql import Db_applications as Db
import settings


WORK_DIR = settings.APP_XMLDIR
LOG_DIR = 'logs/'
MAIN_URL = 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2018/'


def create_logger():
    date = time.strftime('%Y-%m-%d')
    log_file = LOG_DIR + 'tm_parser_' + str(date) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))


def download_html(url):
    print('Downloading %s' % url)
    html_content = None
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/56.0.2924.87 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q = 0.9, image / webp, * / *;q = 0.8"}
    try:
        r = requests.get(url, headers=headers, allow_redirects=True)
        if r.status_code != 200:
            print('[%s] Downloading %s' % r.status_code, url)
            print(r.headers)
            print(r.cookies)
        else:
            html_content = r.content
    except requests.exceptions.RequestException as err:
        print('Failed to open url %s' % url)
        html_content = None
    finally:
        if type(html_content) is bytes:
            return html_content.decode()
        else:
            return html_content


def get_text_or_none(element, item_name):
    item = element.xpath(item_name)
    if len(item) > 0:
        return str(item[0])
    else:
        return None


def print_children(element, level=0):
    if element is None:
        return
    print('* Content of', element)
    print(element.attrib)
    l = []
    for child in element:
        tag = child.tag
        print(tag)
        text = get_text_or_none(child, 'text()')
        if text is not None and text.strip() != '':
            print('\t', text)
        else:
            l.append(child)
    if level > 0:
        try:
            level = level - 1
            for child in l:
                print_children(child, level)
        except Exception:
            pass


def clean_file(raw_file):
    '''
    Alters the raw patentsview data files to produce valid XML. 
    Original is invalid because it lacks a root tag and has multiple xml version tags
    :param raw_file: raw patentsview file
    :return  null, creates list of files
    '''
    logger.info('Cleaning XML file')
    if settings.APP_XMLDIR not in raw_file:
            raw_file = os.path.join(settings.APP_XMLDIR, raw_file)
    new_file = raw_file[:-4] + '_clean.xml'
    file_already_cleaned = False
    try:
        with open(raw_file, 'r+') as f:
            with open(new_file, 'w') as new_f:
                new_f.write('<?xml version="1.0" encoding="UTF-8"?>' + "\n")
                new_f.write('<root>' + '\n')
                for line in f.readlines():
                    if not line.lstrip().startswith('<?xml') and not line.lstrip().startswith('<!DOCTYPE'):
                        if line.lstrip().startswith('<root'):
                            file_already_cleaned = True
                            break
                        new_f.write(line)
                new_f.write('</root>')
        if file_already_cleaned:
            os.remove(new_file)
        else:
            os.replace(new_file, raw_file)
        # move(new_file, raw_file)
    except Exception:
        logger.exception('message')


def download_file(url):
    zip_filename = os.path.join(settings.APP_XMLDIR, url.split('/')[-1])
    xml_filename = zip_filename.replace('zip', 'xml')
    if os.path.isfile(zip_filename) or os.path.isfile(xml_filename):
        logger.debug('File already exists.')
    else:
        logger.info('Downloading %s', url)
        # NOTE the stream=True parameter
        # logger.debug('Getting zip from %s' % url)
        start_time = time.time()
        r = requests.get(url, stream=True)
        with open(zip_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        r.close()
        logger.info('File %s downloaded in [%s sec].', zip_filename, time.time() - start_time)
    if not os.path.isfile(xml_filename) and os.path.isfile(zip_filename):
        try:
            zip_ref = zipfile.ZipFile(zip_filename, 'r')
            zip_ref.extractall(settings.APP_XMLDIR)
            zip_ref.close()
            clean_file(xml_filename)
            os.remove(zip_filename)
        except Exception:
            logger.error('UNZIP ERROR. Deleting file %s' % zip_filename)
            os.remove(zip_filename)
    return xml_filename.split('/')[-1]


def get_urls(main_url):
    html_content = download_html(main_url)
    html_tree = html.fromstring(html_content)
    html_tree.make_links_absolute(main_url)
    content = html_tree.xpath('//div[@class="container"]/table[2]//tr')
    files = ()
    for row in content:
        a = row.xpath('td/a[contains(@href, ".zip")]')
        if len(a) > 0:
            apc = {'url': a[0].get('href'),
                   'size': row.xpath('td[2]//text()')[0],
                   'date_string': row.xpath('td[3]//text()')[0]}
            files = (apc,) + files
    if len(files) > 0:
        logger.info('Found %s XML files online', len(files))
        return files
    return None


def parse_file(filename, file_id):
    dbc = Db()
    if WORK_DIR not in filename:
        filename = os.path.join(settings.APP_XMLDIR, filename)
    with open(filename, 'rb') as inputfile:
        file_start_time = time.time()
        logger.info('Parsing file %s' % filename)
        context = etree.iterparse(inputfile, events=('end',), tag='us-patent-application')
        app_counter = 0
        for event, case in context:
            data_application = case.find('us-bibliographic-data-application')
            app_ref = data_application.find('application-reference')
            app_id = int(get_text_or_none(app_ref, 'document-id/doc-number/text()'))
            app_id_db = dbc.app_id_get(app_id, file_id)
            if app_id_db is not None:
                logger.info('APP_id %s do not exists in database and will be inserted', app_id)
                new_file_date = int(re.sub(r"\D", "", filename))
                db_file_date = int(re.sub(r"\D", "", app_id_db['filename']))
                print(new_file_date, db_file_date)
                if new_file_date > db_file_date \
                        or app_id_db['status'] is False \
                        or (new_file_date >= db_file_date and args.parseall and args.force):
                    for t in ('trademark_app_case_files', 'trademark_app_case_file_event_statements',
                              'trademark_app_case_file_headers', 'trademark_app_case_file_owners',
                              'trademark_app_case_file_statements', 'trademark_app_classifications',
                              'trademark_app_correspondents', 'trademark_app_design_searches',
                              'trademark_app_foreign_applications', 'trademark_app_international_registration',
                              'trademark_app_madrid_history_events', 'trademark_app_madrid_international_filing_record',
                              'trademark_app_prior_registration_applications', 'trademark_app_us_codes'):
                        dbc.delete_serial(app_id, t)
                    logger.info('Processing existing serial number %s', app_id)
                    parse_case(case, app_id, file_id)
            else:
                logger.info('Processing new app_id %s', app_id)
                parse_app(case, app_id, filename)
            app_counter += 1
            case.clear()
            if app_counter == 5:
                sys.exit()
    dbc.file_update_status(file_id, 'finished')
    os.remove(filename)
    logger.info('Finished parsing file %s in [%s sec]', filename, time.time() - file_start_time)


def parse_app(case, app_id, filename):
    data_application = case.find('us-bibliographic-data-application')
    app_ref = data_application.find('application-reference')
    pub_ref = data_application.find('publication-reference')
    claims_element_list = case.findall('claims/claim')
    # print_children(data_application.find('us-parties'), 3)
    rawlocation_list = []

    def parse_application():
        dbc = Db()

        pub_date = get_text_or_none(pub_ref, 'document-id/date/text()')
        if pub_date[6:] != "00":
            pub_date = pub_date[:4] + '-' + pub_date[4:6] + '-' + pub_date[6:]
            year = pub_date[:4]
        else:
            pub_date = pub_date[:4] + '-' + pub_date[4:6] + '-' + '01'
            year = pub_date[:4]

        abstract_p_list = case.findall('abstract/p')
        abstract = ''
        for p in abstract_p_list:
            abstract += get_text_or_none(p, 'text()')

        application = {
            'id': year + '/' + get_text_or_none(pub_ref, 'document-id/doc-number/text()'),
            'type': app_ref.attrib['appl-type'],
            'number': get_text_or_none(pub_ref, 'document-id/doc-number/text()'),
            'app_id': app_id,
            'country': get_text_or_none(app_ref, 'document-id/country/text()'),
            'date': pub_date,
            'abstract': abstract,
            'title': get_text_or_none(case, 'us-bibliographic-data-application/invention-title/text()'),
            'granted': None,
            'num_claims': len(claims_element_list),
            'filename': filename.split('\\')[-1],
        }
        return application

    def parse_claims():
        dbc = Db()
        claims_list = []
        for claim_element in claims_element_list:
            sequence = claim_element.attrib['num']
            claim_text_full = etree.tostring(claim_element).decode()
            dependent = re.search('<claim-ref idref="CLM-(\d+)">', claim_text_full)
            dependent = int(dependent.group(1)) if dependent is not None else None
            text = re.sub('<.*?>|</.*?>', '', claim_text_full)
            text = re.sub('[\n\t\r\f]+', '', text)
            text = re.sub('^\d+\.\s+', '', text)
            text = re.sub('\s+', ' ', text)
            claim = {
                'uuid': id_generator(),
                'application_id': application['id'],
                'app_id': application['app_id'],
                'text': text,
                'dependent': dependent,
                'sequence': sequence
            }
            claims_list.append(claim)
        # print(claims_list)

    def parse_description():
        dbc = Db()
        description_element = case.find('description')
        description_text_full = etree.tostring(description_element).decode()
        text = re.sub('<.*?>|</.*?>', '', description_text_full)
        text = re.sub('[\n\t\r\f]+', ' ', text)
        text = re.sub('^\d+\.\s+', '', text)
        text = re.sub('\s+', ' ', text)
        description = {
            'app_id': app_id,
            'uuid': id_generator(),
            'text': text
        }

    def parse_assignees():
        assignees_element_list = data_application.findall('assignees/assignee')
        rawassignee_list = []
        for assignee_element in assignees_element_list:

            loc_idd = id_generator()
            rawassignee = {
                'uuid': id_generator(),
                'application_id': application['id'],
                'app_id': app_id,
                'assignee_id': None,
                'rawlocation_id': loc_idd,
                'type': get_text_or_none(assignee_element, 'addressbook/role/text()'),
                'name_first': get_text_or_none(assignee_element, 'addressbook/first-name/text()'),
                'name_last': get_text_or_none(assignee_element, 'addressbook/last-name/text()'),
                'organization': get_text_or_none(assignee_element, 'addressbook/orgname/text()'),
                'sequence': None
            }
            rawassignee_list.append(rawassignee)

            rawlocation = {
                'id': id_generator(),
                'location_id': loc_idd,
                'city': get_text_or_none(assignee_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(assignee_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(assignee_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(assignee_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)
    
    def parse_inventors():
        inventors_element_list = data_application.findall('us-parties/inventors/inventor')
        rawinventors_list = []

        for inventor_element in inventors_element_list:
            loc_idd = id_generator()
            rawinventor = {
                'uuid': id_generator(),
                'application_id': application['id'],
                'app_id': app_id,
                'inventor_id': None,
                'rawlocation_id': loc_idd,
                'name_first': get_text_or_none(inventor_element, 'addressbook/first-name/text()'),
                'name_last': get_text_or_none(inventor_element, 'addressbook/last-name/text()'),
                'sequence': int(inventor_element.attrib['sequence'])
            }
            rawinventors_list.append(rawinventor)

            rawlocation = {
                'id': id_generator(),
                'location_id': loc_idd,
                'city': get_text_or_none(inventor_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(inventor_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)
    
    def parse_applicants():
        print_children(data_application.find('us-parties/us-applicants'), 3)
        applicants_element_list = data_application.findall('us-parties/us-applicants/us-applicant')
        applicants_list = []
        exit()
        for applicant_element in applicants_element_list:
            loc_idd = id_generator()
            applicant = {
                'uuid': id_generator(),
                'application_id': application['id'],
                'app_id': app_id,
                'inventor_id': None,
                'rawlocation_id': loc_idd,
                'name_first': get_text_or_none(applicant_element, 'addressbook/first-name/text()'),
                'name_last': get_text_or_none(applicant_element, 'addressbook/last-name/text()'),
                'organization': get_text_or_none(applicant_element, 'addressbook/orgname/text()'),
                'sequence': int(inventor_element.attrib['sequence']),
                'designation': None,
                'applicant_type': None,
            }
            applicants_list.append(rawinventor)

            rawlocation = {
                'id': id_generator(),
                'location_id': loc_idd,
                'city': get_text_or_none(inventor_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(inventor_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)

    dbc = Db()
    start_time = time.time()

    application = parse_application()
    # print(application)
    parse_claims()
    parse_description()
    parse_assignees()
    parse_inventors()
    parse_non_inventors()

    # with cf.ThreadPoolExecutor(max_workers=12) as executor:
    #     executor.submit(parse_case_files)
    #     executor.submit(parse_headers)
    #     executor.submit(parse_statements)
    #     executor.submit(parse_event_statements)
    #     executor.submit(parse_prior_registration_applications)
    #     executor.submit(parse_foreign_applications)
    #     executor.submit(parse_classifications)
    #     executor.submit(parse_correspondents)
    #     executor.submit(parse_owners)
    #     executor.submit(parse_design_searches)
    #     executor.submit(parse_international_registration)
    #     executor.submit(parse_madrid_international_filing_record)

    # dbc.case_file_update_status(doc_id, 'true')
    logger.info('Inserted application %s in [%s sec]', app_id, time.time() - start_time)


def main_worker(file):
    dbc = Db()
    file_check = dbc.file_check(file)
    if file_check is None:
        xml_filename = download_file(file['url'])
        if xml_filename is not None:
            inserted_id = dbc.file_insert(file, xml_filename)
            parse_file(xml_filename, inserted_id)
    elif file_check['status'] in ['new', ''] or file_check['status'] is None:
        logger.warning('File %s exists into database. Going to process again', file_check['filename'])
        if not os.path.isfile(os.path.join(WORK_DIR, file_check['filename'])):
            xml_filename = download_file(file['url'])
        else:
            xml_filename = file_check['filename']
        if settings.APP_XMLDIR not in xml_filename:
            xml_filename = os.path.join(settings.APP_XMLDIR, xml_filename)
        parse_file(xml_filename, file_check['id'])
    else:
        logger.info('File %s is already inserted into database.', file_check['filename'])
        if args.parse:
            logger.info('Nothing to work. Exiting.')
            sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Downloads, parses and inserts to database patent applications.')
    parser.add_argument('--parse', help='Parses most recent data.', action="store_true")
    parser.add_argument('--parseall', help='Parses all the data.', action="store_true")
    parser.add_argument('--force', help='Forces to discard old data, use with --parseall command.', action="store_true")
    args = parser.parse_args()
    if args.parse or args.parseall:
        os.makedirs(os.path.dirname(WORK_DIR), exist_ok=True)
        os.makedirs(os.path.dirname(LOG_DIR), exist_ok=True)
        logger = create_logger()
        files_tuple = get_urls(MAIN_URL)
        # single-thread test
        for file in files_tuple:
            main_worker(file)
        sys.exit()
        with cf.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(main_worker, files_tuple)
    else:
        parser.print_help()
        sys.exit()
