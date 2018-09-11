import argparse
import concurrent.futures as cf
from itertools import repeat
import logging
import logging.config
import os
from pprint import pprint
import random
import re
import requests
import string
import sys
import time
import uuid
import zipfile
from lxml import etree, html

from db_pgsql import Db_grants as Db
import settings


WORK_DIR = settings.GRANT_XMLDIR
LOG_DIR = 'logs/'
MAIN_URL = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/2017/'
MAIN_URL = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/2018/'


def create_logger():
    date = time.strftime('%Y-%m-%d')
    log_file_name = LOG_DIR + 'grant_parser_' + str(date) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file_name})
    return logging.getLogger(__name__)


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


def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))


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
    if WORK_DIR not in raw_file:
            raw_file = os.path.join(WORK_DIR, raw_file)
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
    zip_filename = os.path.join(WORK_DIR, url.split('/')[-1])
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
            zip_ref.extractall(WORK_DIR)
            zip_ref.close()
            clean_file(xml_filename)
            os.remove(zip_filename)
        except Exception:
            logger.error('UNZIP ERROR. Deleting file %s' % zip_filename)
            os.remove(zip_filename)
    return os.path.basename(xml_filename)


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
        filename = os.path.join(WORK_DIR, filename)
    with open(filename, 'rb') as inputfile:
        file_start_time = time.time()
        logger.info('Parsing file %s' % filename)
        context = etree.iterparse(inputfile, events=('end',), tag='us-patent-grant')
        pat_counter = 1
        for event, case in context:
            data_grant = case.find('us-bibliographic-data-grant')
            pub_ref = data_grant.find('publication-reference')
            docno = get_text_or_none(pub_ref, 'document-id/doc-number/text()')
            num = re.findall('\d+', docno)
            num = num[0]  # turns it from list to string
            if num[0].startswith("0"):
                num = num[1:]
                let = re.findall('[a-zA-Z]+', docno)
            else:
                let = None
            if let:
                let = let[0]  # list to string
                docno = let + num
            else:
                docno = num
            patent_id = docno
            print(pat_counter, '[{}:{}]'.format(os.path.basename(filename), patent_id))
            patent_id_db = dbc.patent_id_get(patent_id, file_id)
            if patent_id_db is not None:
                logger.warning('Patent_id %s exists in the database', patent_id)
                new_file_date = int(re.sub(r"\D", "", filename))
                db_file_date = int(re.sub(r"\D", "", patent_id_db['filename']))
                # print(new_file_date, db_file_date)
                # exit()
                if new_file_date > db_file_date \
                    or (patent_id_db['status'] == 'new' and args.force) \
                    or (new_file_date >= db_file_date and args.parseall and args.force):
                    table_names = [
                        'claim', 'ipcr', 'uspc', 'uspatentcitation', 'wipo', 'government_interest',
                        'usapplicationcitation', 'foreigncitation', 'otherreference', 'rawassignee',
                        'non_inventor_applicant', 'rawinventor', 'rawlawyer', 'rawexaminer', 'usreldoc',
                        'foreign_priority', 'draw_desc_text', 'rel_app_text', 'brf_sum_text', 'detail_desc_text',
                        'us_term_of_grant', 'pct_data', 'figures', 'botanic', 'application', 'patent'
                    ]
                    logger.warning('Deleting existing patent with id %s', patent_id)
                    # for t in table_names:
                    #     dbc.delete_patent(patent_id, t)
                    with cf.ThreadPoolExecutor(max_workers=len(table_names)) as executor:
                        executor.map(dbc.delete_patent, repeat(patent_id), table_names)
                    dbc.cnx.commit()
                    parse_grant(case, filename, dbc)
            else:
                logger.info('Processing new Patent_id %s', patent_id)
                parse_grant(case, filename, dbc)
            pat_counter += 1
            case.clear()
    dbc.file_update_status(file_id, 'finished')
    os.remove(filename)
    logger.info('Finished parsing file %s in [%s sec]', filename, time.time() - file_start_time)


def parse_grant(case, filename, dbc):
    global rawlocation_list
    global mainclassdata
    global subclassdata
    data_grant = case.find('us-bibliographic-data-grant')
    pub_ref = data_grant.find('publication-reference')
    app_ref = data_grant.find('application-reference')
    docno = get_text_or_none(pub_ref, 'document-id/doc-number/text()')
    num = re.findall('\d+', docno)
    num = num[0]  # turns it from list to string
    if num[0].startswith("0"):
        num = num[1:]
        let = re.findall('[a-zA-Z]+', docno)
    else:
        let = None
    if let:
        let = let[0]  # list to string
        docno = let + num
    else:
        docno = num
    patent_id = docno
    app_id = int(get_text_or_none(app_ref, 'document-id/doc-number/text()'))
    # rawinventor_list = []
    mainclass_list = []
    subclass_list = []
    rule_47_flag = data_grant.find('rule-47-flag')
    if rule_47_flag is not None:
        rule_47 = 1
    else:
        rule_47 = 0

    def parse_application():
        app_date = get_text_or_none(app_ref, 'document-id/date/text()')
        if app_date[6:] != "00":
            app_date = app_date[:4] + '-' + app_date[4:6] + '-' + app_date[6:]
            year = app_date[:4]
        else:
            app_date = app_date[:4] + '-' + app_date[4:6] + '-' + '01'
            year = app_date[:4]

        series_code = get_text_or_none(data_grant, 'us-application-series-code/text()')

        application = {
            'app_id': app_id,
            'id': year + '/' + get_text_or_none(app_ref, 'document-id/doc-number/text()'),
            'patent_id': patent_id,
            'type': int(series_code),
            'number': get_text_or_none(app_ref, 'document-id/doc-number/text()'),
            'country': get_text_or_none(app_ref, 'document-id/country/text()'),
            'date': app_date,
            'id_transformed': str(series_code) + '/' + str(app_id)[2:],
            'number_transformed': app_id,
            'series_code_transformed_from_type ': series_code,
        }
        try:
            dbc.insert_dict(application, 'application')
        except Exception:
            raise
        return application

    def parse_patent():
        pubdate = get_text_or_none(pub_ref, 'document-id/date/text()')
        if pubdate[6:] != "00":
            pubdate = pubdate[:4] + '-' + pubdate[4:6] + '-' + pubdate[6:]
        else:
            pubdate = pubdate[:4] + '-' + pubdate[4:6] + '-' + '01'
            year = pubdate[:4]

        abstract_p_list = case.findall('abstract/p')
        abstract = ''
        for p in abstract_p_list:
            abstract += get_text_or_none(p, 'text()')

        patent = {
            'app_id': app_id,
            'id': patent_id,
            'type': app_ref.attrib['appl-type'],
            'number': get_text_or_none(pub_ref, 'document-id/doc-number/text()'),
            'country': get_text_or_none(pub_ref, 'document-id/country/text()'),
            'date': pubdate,
            'abstract': abstract,
            'title': get_text_or_none(data_grant, 'invention-title/text()'),
            'kind': get_text_or_none(pub_ref, 'document-id/kind/text()'),
            'color_status': 0,
            'num_claims': int(get_text_or_none(data_grant, 'number-of-claims/text()')),
            'filename': os.path.basename(filename)
        }
        try:
            dbc.insert_dict(patent, 'patent')
            if patent['type'] == 'plant':
                wipo = {
                    'app_id': app_id,
                    'patent_id': patent_id,
                    'field_id': 36,
                    'sequence': 0
                }
                dbc.insert_dict(wipo, 'wipo')
        except Exception:
            raise
        return patent

    def parse_claims():
        exemplary_claims = []
        exemplary_claims_element_list = data_grant.findall('us-exemplary-claim')
        for exeplary_claim in exemplary_claims_element_list:
            exemplary_claims.append(get_text_or_none(exeplary_claim, 'text()'))

        claims_element_list = case.findall('claims/claim')
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
            exemplary = False
            if sequence.lstrip("0") in exemplary_claims:
                exemplary = True
            claim = {
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'app_id': app_id,
                'text': text,
                'dependent': dependent,
                'sequence': int(sequence),
                'exemplary': exemplary
            }
            claims_list.append(claim)
        dbc.insert_listdict(claims_list, 'claim')

    def parse_ipcr():
        ipcr_list = []
        ipcr_element_list = data_grant.findall('classifications-ipcr/classification-ipcr')
        sequence = 0
        for ipcr_element in ipcr_element_list:
            action_date = get_text_or_none(ipcr_element, 'action-date/date/text()')
            if action_date[6:] != "00":
                action_date = action_date[:4] + '-' + action_date[4:6] + '-' + action_date[6:]
            else:
                action_date = action_date[:4] + '-' + action_date[4:6] + '-' + '01'

            ipcrversion = get_text_or_none(ipcr_element, 'ipc-version-indicator/date/text()')
            if ipcrversion[6:] != "00":
                ipcrversion = ipcrversion[:4] + '-' + ipcrversion[4:6] + '-' + ipcrversion[6:]
            else:
                ipcrversion = ipcrversion[:4] + '-' + ipcrversion[4:6] + '-' + '01'

            ipcr = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'classification_level': get_text_or_none(ipcr_element, 'classification-level/text()'),
                'section': get_text_or_none(ipcr_element, 'section/text()'),
                'ipc_class': get_text_or_none(ipcr_element, 'class/text()'),
                'subclass': get_text_or_none(ipcr_element, 'subclass/text()'),
                'main_group': get_text_or_none(ipcr_element, 'main-group/text()'),
                'subgroup': get_text_or_none(ipcr_element, 'subgroup/text()'),
                'symbol_position': get_text_or_none(ipcr_element, 'symbol-position/text()'),
                'classification_value': get_text_or_none(ipcr_element, 'classification-value/text()'),
                'classification_status': get_text_or_none(ipcr_element, 'classification-status/text()'),
                'classification_data_source': get_text_or_none(ipcr_element, 'classification-data-source/text()'),
                'action_date': action_date,
                'ipc_version_indicator': ipcrversion,
                'sequence': sequence
            }
            ipcr_list.append(ipcr)
            sequence += 1
        dbc.insert_listdict(ipcr_list, 'ipcr')

    def parse_uspc():
        uspc_list = []
        uspc_element_list = data_grant.findall('classification-national')
        sequence = 0
        for uspc_element_list in uspc_element_list:
            main = get_text_or_none(uspc_element_list, 'main-classification/text()')
            if main:
                crossrefsub = main[3:].replace(" ", "")
                if len(crossrefsub) > 3 and re.search('^[A-Z]', crossrefsub[3:]) is None:
                    crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
                crossrefsub = re.sub('^0+', '', crossrefsub)
                if re.search('[A-Z]{3}', crossrefsub[:3]):
                    crossrefsub = crossrefsub.replace(".", "")
                main = main[:3].replace(" ", "")
                sub = crossrefsub
                mainclassdata[main] = main
                subclassdata[sub] = sub
            further_class = get_text_or_none(uspc_element_list, 'further-classification/text()')
            if further_class:
                further_sub_class = further_class[3:].replace(" ", "")
                if len(further_sub_class) > 3 and re.search('^[A-Z]', further_sub_class[3:]) is None:
                    further_sub_class = further_sub_class[:3] + '.'+further_sub_class[3:]
                further_sub_class = re.sub('^0+', '', further_sub_class)
                if re.search('[A-Z]{3}', further_sub_class[:3]):
                    further_sub_class = further_sub_class.replace(".", "")
                further_class = further_class[:3].replace(" ", "")
                mainclassdata[further_class] = further_class
                if further_sub_class != "":
                    uspc = {
                        'app_id': app_id,
                        'uuid': str(uuid.uuid1()),
                        'patent_id': patent_id,
                        'mainclass_id': further_class,
                        'subclass_id': further_class + '/' + further_sub_class,
                        'sequence': sequence
                    }
                    uspc_list.append(uspc)
                    if sequence == 0 and further_class.startswith('D') and patent['type'] == 'design':
                        wipo = {
                            'app_id': app_id,
                            'patent_id': patent_id,
                            'field_id': further_class,
                            'sequence': 0
                        }
                        dbc.insert_dict(wipo, 'wipo')
                    sequence += 1
        dbc.insert_listdict(uspc_list, 'uspc')

    def parse_citations():
        # in 2021 schema changed to us-references-cited instead of references-cited
        citation_element_list = data_grant.findall('references-cited/citation')
        uspatentcitation_list = []
        usappcitation_list = []
        foreigncitation_list = []
        otherreference_list = []
        if len(citation_element_list) == 0:
            citation_element_list = data_grant.findall('us-references-cited/us-citation')
        uspat_seq = 0
        appseq = 0
        forpatseq = 0
        otherseq = 0
        for citation_element in citation_element_list:
            citcountry = get_text_or_none(citation_element, 'patcit/document-id/country/text()')
            citdocno = get_text_or_none(citation_element, 'patcit/document-id/doc-number/text()')
            # figure out if this is a citation to an application
            app_flag = False
            if citdocno and re.match(r'^[A-Z]*\d+$', citdocno):
                num = re.findall('\d+', citdocno)
                num = num[0]  # turns it from list to string
                if num[0] == '0':  # drop leading zeros
                    num = num[1:]
                let = re.findall('[a-zA-Z]+', citdocno)
                if let:
                    let = let[0]  # list to string
                    citdocno = let + num
                else:
                    citdocno = num
            else:  # basically if there is anything other than number and digits
                citdocno = citdocno
                app_flag = True

            citdate = get_text_or_none(citation_element, 'patcit/document-id/date/text()')
            if citdate:
                if citdate[6:] != "00":
                    citdate = citdate[:4] + '-' + citdate[4:6] + '-' + citdate[6:]
                elif citdate[4:6] !="00":
                    citdate = citdate[:4] + '-' + citdate[4:6] + '-' + '01'
                else:
                    citdate = citdate[:4] + '-' + '01' + '-' + '01'

            text = get_text_or_none(citation_element, 'nplcit/othercit/text()')

            if citcountry == "US":
                if citdocno and not app_flag:
                    uspatentcitation = {
                        'app_id': app_id,
                        'uuid': str(uuid.uuid1()),
                        'patent_id': patent_id,
                        'citation_id': citdocno,
                        'date': citdate,
                        'name': get_text_or_none(citation_element, 'patcit/document-id/name/text()'),
                        'kind': get_text_or_none(citation_element, 'patcit/document-id/kind/text()'),
                        'country': citcountry,
                        'category': get_text_or_none(citation_element, 'category/text()'),
                        'sequence': uspat_seq
                    }
                    uspatentcitation_list.append(uspatentcitation)
                    uspat_seq += 1
                if citdocno != 'NULL' and app_flag:
                    usappcitation = {
                        'app_id': app_id,
                        'uuid': str(uuid.uuid1()),
                        'patent_id': patent_id,
                        'application_id': citdocno,
                        'date': citdate,
                        'name': get_text_or_none(citation_element, 'patcit/document-id/name/text()'),
                        'kind': get_text_or_none(citation_element, 'patcit/document-id/kind/text()'),
                        'number': citdocno,
                        'country': citcountry,
                        'category': get_text_or_none(citation_element, 'category/text()'),
                        'sequence': appseq,
                        'application_id_transformed': None,
                        'number_transformed': None
                    }
                    usappcitation_list.append(usappcitation)
                    appseq += 1

            elif citdocno:
                foreigncitation = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'date': citdate,
                    'number': citdocno,
                    'country': citcountry,
                    'category': get_text_or_none(citation_element, 'category/text()'),
                    'sequence': forpatseq
                }
                foreigncitation_list.append(foreigncitation)
                forpatseq += 1
            if text:
                otherreference = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'text': text,
                    'sequence': otherseq
                }
                otherreference_list.append(otherreference)
                otherseq += 1
        dbc.insert_listdict(uspatentcitation_list, 'uspatentcitation')
        dbc.insert_listdict(usappcitation_list, 'usapplicationcitation')
        dbc.insert_listdict(foreigncitation_list, 'foreigncitation')
        dbc.insert_listdict(otherreference_list, 'otherreference')

    def parse_assignees():
        assignees_element_list = data_grant.findall('assignees/assignee')
        rawassignee_list = []
        sequence = 0
        for assignee_element in assignees_element_list:
            loc_idd = str(uuid.uuid1())
            role = get_text_or_none(assignee_element, 'addressbook/role/text()')
            role_type = None
            if role:
                role_type = int(role)
            else:
                role = get_text_or_none(assignee_element, 'role/text()')
                role_type = int(role)

            rawassignee = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'assignee_id': None,
                'rawlocation_id': loc_idd,
                'type': role_type,
                'name_first': get_text_or_none(assignee_element, 'addressbook/first-name/text()'),
                'name_last': get_text_or_none(assignee_element, 'addressbook/last-name/text()'),
                'organization': get_text_or_none(assignee_element, 'addressbook/orgname/text()'),
                'city': get_text_or_none(assignee_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(assignee_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(assignee_element, 'addressbook/address/country/text()'),
                'sequence': sequence
            }
            rawassignee_list.append(rawassignee)
            sequence += 1

            rawlocation = {
                'id': loc_idd,
                'location_id': None,
                'city': get_text_or_none(assignee_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(assignee_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(assignee_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(assignee_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)
        dbc.insert_listdict(rawassignee_list, 'rawassignee')

    def parse_non_inventor():
        applicants_element_list = data_grant.findall('us-parties/us-applicants/us-applicant')
        non_inventor_app_list = []
        rawinventor_list = []
        sequence = 0
        inv_seq = 0
        for applicant_element in applicants_element_list:
            loc_idd = id_generator()
            rawlocation = {
                'id': loc_idd,
                'location_id': None,
                'city': get_text_or_none(applicant_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(applicant_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(applicant_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(applicant_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)

            # this get us the non-inventor applicants in 2013+. Inventor applicants are in applicant and also in inventor.
            non_inventor_app_types = ['legal-representative', 'party-of-interest', 'obligated-assignee', 'assignee']
            earlier_applicant_type = applicant_element.attrib['app-type']
            try:
                later_applicant_type = applicant_element.attrib['applicant-authority-category']
            except KeyError:
                later_applicant_type = None
            if later_applicant_type in non_inventor_app_types:
                non_inventor_applicant = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'rawlocation_id': loc_idd,
                    'lname': get_text_or_none(applicant_element, 'addressbook/last-name/text()'),
                    'fname': get_text_or_none(applicant_element, 'addressbook/first-name/text()'),
                    'organization': get_text_or_none(applicant_element, 'addressbook/orgname/text()'),
                    'sequence': sequence,
                    'designation': applicant_element.attrib['designation'],
                    'applicant_type': later_applicant_type
                }
                non_inventor_app_list.append(non_inventor_applicant)
                sequence += 1

            # this gets us the inventors from 2005-2012
            if earlier_applicant_type and earlier_applicant_type == "applicant-inventor":
                rawinventor = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'inventor_id': None,
                    'rawlocation_id': loc_idd,
                    'name_first': get_text_or_none(applicant_element, 'addressbook/first-name/text()'),
                    'name_last': get_text_or_none(applicant_element, 'addressbook/last-name/text()'),
                    'city': get_text_or_none(applicant_element, 'addressbook/address/city/text()'),
                    'state': get_text_or_none(applicant_element, 'addressbook/address/state/text()'),
                    'country': get_text_or_none(applicant_element, 'addressbook/address/country/text()'),
                    'sequence': inv_seq,
                    'rule_47': rule_47
                }
                rawinventor_list.append(rawinventor)
                inv_seq += 1
            elif earlier_applicant_type and earlier_applicant_type != "applicant-inventor":
                non_inventor_applicant = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'rawlocation_id': loc_idd,
                    'lname': get_text_or_none(applicant_element, 'addressbook/last-name/text()'),
                    'fname': get_text_or_none(applicant_element, 'addressbook/first-name/text()'),
                    'organization': get_text_or_none(applicant_element, 'addressbook/orgname/text()'),
                    'sequence': sequence,
                    'designation': applicant_element.attrib['designation'],
                    'applicant_type': earlier_applicant_type
                }
                non_inventor_app_list.append(non_inventor_applicant)
                sequence += 1
        dbc.insert_listdict(non_inventor_app_list, 'non_inventor_applicant')
        dbc.insert_listdict(rawinventor_list, 'rawinventor')

    def parse_inventors():
        # 2005-2013 (inclusive) same of the inventor is only stored in inventor if the inventor is not also the applicant in earlier year
        # mostly this is because the inventor is dead
        # so inventors has dead /incapacitated invenntors adn "applicant " has live

        # 2013 on name of the inventor is stored in the inventors block always
        # split on live or deceased inventor
        inventors_element_list = data_grant.findall('us-parties/inventors/inventor')
        rawinventor_list = []
        sequence = 0
        for inventor_element in inventors_element_list:
            loc_idd = str(uuid.uuid1())
            rawinventor = {
                    'app_id': app_id,
                    'uuid': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'inventor_id': None,
                    'rawlocation_id': loc_idd,
                    'name_first': get_text_or_none(inventor_element, 'addressbook/first-name/text()'),
                    'name_last': get_text_or_none(inventor_element, 'addressbook/last-name/text()'),
                    'city': get_text_or_none(inventor_element, 'addressbook/address/city/text()'),
                    'state': get_text_or_none(inventor_element, 'addressbook/address/state/text()'),
                    'country': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
                    'sequence': sequence,
                    'rule_47': rule_47
                }
            rawinventor_list.append(rawinventor)
            sequence += 1

            rawlocation = {
                'id': loc_idd,
                'location_id': None,
                'city': get_text_or_none(inventor_element, 'addressbook/address/city/text()'),
                'state': get_text_or_none(inventor_element, 'addressbook/address/state/text()'),
                'country': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
                'country_transformed': get_text_or_none(inventor_element, 'addressbook/address/country/text()'),
            }
            rawlocation_list.append(rawlocation)
        dbc.insert_listdict(rawinventor_list, 'rawinventor')

    def parse_agents():
        agents_element_list = data_grant.findall('us-parties/agents/agent')
        rawlawyer_list = []
        sequence = 0
        for agent_element in agents_element_list:
            rawlawyer = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'lawyer_id': None,
                'patent_id': patent_id,
                'name_first': get_text_or_none(agent_element, 'addressbook/first-name/text()'),
                'name_last': get_text_or_none(agent_element, 'addressbook/last-name/text()'),
                'organization': get_text_or_none(agent_element, 'addressbook/orgname/text()'),
                'country': get_text_or_none(agent_element, 'addressbook/address/country/text()'),
                'sequence': sequence
            }
            rawlawyer_list.append(rawlawyer)
            sequence += 1
        dbc.insert_listdict(rawlawyer_list, 'rawlawyer')

    def parse_examiners():
        primary_ex_el_ls = data_grant.findall('examiners/primary-examiner')
        assist_ex_el_ls = data_grant.findall('examiners/assistant-examiner')
        rawexaminer_list = []
        if len(primary_ex_el_ls) > 0:
            for examiner in primary_ex_el_ls:
                rawexaminer = {
                    'app_id': app_id,
                    'id': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'fname': get_text_or_none(examiner, 'first-name/text()'),
                    'lname': get_text_or_none(examiner, 'last-name/text()'),
                    'role': "primary",
                    '"group"': get_text_or_none(examiner, 'department/text()')
                }
                rawexaminer_list.append(rawexaminer)
        if len(assist_ex_el_ls) > 0:
            for examiner in primary_ex_el_ls:
                rawexaminer = {
                    'app_id': app_id,
                    'id': str(uuid.uuid1()),
                    'patent_id': patent_id,
                    'fname': get_text_or_none(examiner, 'first-name/text()'),
                    'lname': get_text_or_none(examiner, 'last-name/text()'),
                    'role': "assistant",
                    '"group"': get_text_or_none(examiner, 'department/text()')
                }
                rawexaminer_list.append(rawexaminer)
        dbc.insert_listdict(rawexaminer_list, 'rawexaminer')

    def parse_related_docs():
        related_docs = data_grant.find('us-related-documents')
        possible_doc_type = [
            'division', 'continuation', 'continuation-in-part', 'continuing-reissue', 'us-divisional-reissue',
            'reexamination', 'substitution', 'us-provisional-application', 'utility-model-basis', 'reissue',
            'related-publication', 'correction', 'us-provisional-application', 'us-reexamination-reissue-merger']
        possible_relations = ['parent-doc', 'parent-grant-document', 'parent-pct-document', 'child-doc']
        usreldoc_list = []
        sequence = 0
        if related_docs is not None:
            for doc_type in possible_doc_type:
                doc_type_element = related_docs.find(doc_type)
                if doc_type_element is not None:
                    if (doc_type == "us-provisional-application") | (doc_type == "related-publication"):
                        reldate = get_text_or_none(doc_type_element, 'document-id/date/text()')
                        if reldate and reldate[6:] != "00":
                            reldate = reldate[:4] + '-' + reldate[4:6] + '-' + reldate[6:]
                        else:
                            reldate = reldate[:4] + '-' + reldate[4:6] + '-' + '01'

                        usreldoc = {
                            'app_id': app_id,
                            'uuid': str(uuid.uuid1()),
                            'patent_id': patent_id,
                            'doctype': doc_type,
                            'relkind': None,
                            'reldocno': get_text_or_none(doc_type_element, 'document-id/doc-number/text()'),
                            'country': get_text_or_none(doc_type_element, 'document-id/country/text()'),
                            'date': reldate,
                            'status': None,
                            'sequence': sequence,
                            'kind': get_text_or_none(doc_type_element, 'document-id/kind/text()')
                        }
                        usreldoc_list.append(usreldoc)
                        sequence += 1
                    else:
                        for rel_doc in possible_relations:
                            relation = 'relation/' + rel_doc
                            rel_doc_element = doc_type_element.find(relation)
                            if rel_doc_element is not None:
                                reldate = get_text_or_none(rel_doc_element, 'document-id/date/text()')
                                if reldate and reldate[6:] != "00":
                                    reldate = reldate[:4] + '-' + reldate[4:6] + '-' + reldate[6:]
                                elif reldate:
                                    reldate = reldate[:4] + '-' + reldate[4:6] + '-' + '01'

                                usreldoc = {
                                    'app_id': app_id,
                                    'uuid': str(uuid.uuid1()),
                                    'patent_id': patent_id,
                                    'doctype': doc_type,
                                    'relkind': rel_doc,
                                    'reldocno': get_text_or_none(rel_doc_element, 'document-id/doc-number/text()'),
                                    'country': get_text_or_none(rel_doc_element, 'document-id/country/text()'),
                                    'date': reldate,
                                    'status': get_text_or_none(rel_doc_element, 'parent-status/text()'),
                                    'sequence': sequence,
                                    'kind': get_text_or_none(rel_doc_element, 'document-id/kind/text()')
                                }
                                usreldoc_list.append(usreldoc)
                                sequence += 1
            dbc.insert_listdict(usreldoc_list, 'usreldoc')

    def parse_foreign_priority():
        priority_claim_element_list = data_grant.findall('priority-claims/priority-claim')
        sequence = 0
        foreign_pri_list = []
        for claim in priority_claim_element_list:
            date = get_text_or_none(claim, 'date/text()')
            if date[6:] != "00":
                date = date[:4] + '-' + date[4:6] + '-' + date[6:]
            else:
                date = date[:4] + '-' + date[4:6] + '-' + '01'

            foreign_pri = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'sequence': sequence,
                'kind': claim.attrib['kind'],
                'number': get_text_or_none(claim, 'doc-number/text()'),
                'date': date,
                'country': get_text_or_none(claim, 'country/text()'),
                'country_transformed': get_text_or_none(claim, 'country/text()')
            }
            foreign_pri_list.append(foreign_pri)
            sequence += 1
        dbc.insert_listdict(foreign_pri_list, 'foreign_priority')

    def parse_draw_desc_text():
        draw_desc_list = []
        sequence = 0
        draw_des_element_list = case.findall('description/description-of-drawings/p')
        for draw_desc_element in draw_des_element_list:
            draw_desc_text_full = etree.tostring(draw_desc_element).decode()
            text = re.sub('<.*?>|</.*?>', '', draw_desc_text_full)
            draw_desc_text = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'text': text,
                'sequence': int(draw_desc_element.attrib['num'])
            }
            draw_desc_list.append(draw_desc_text)
            sequence += 1
        dbc.insert_listdict(draw_desc_list, 'draw_desc_text')

    def parse_rel_app_text():
        description_element = case.find('description')
        description_text_full = etree.tostring(description_element).decode()
        rel_app_text_list = []
        rel_app_containers = description_text_full.split("<?RELAPP")
        if len(rel_app_containers) > 1:
            rel_app = rel_app_containers[1].split("\n")
            rel_app_seq = 0
            text_field = ""
            for line in rel_app:
                if line.startswith("<heading"):
                    heading = re.search(">(.*?)<", line).group(1)
                    text_field += heading + " "
                    rel_app_text = {
                        'app_id': app_id,
                        'uuid': str(uuid.uuid1()),
                        'patent_id': patent_id,
                        'text': text_field,
                        'sequence': rel_app_seq
                    }
                    rel_app_text_list.append(rel_app_text)
                    rel_app_seq += 1
                if line.startswith("<p"):
                    text = re.search(">(.*?)</p>", line).group(1)
                    text_field += text + " "
                    rel_app_text = {
                        'app_id': app_id,
                        'uuid': str(uuid.uuid1()),
                        'patent_id': patent_id,
                        'text': text_field,
                        'sequence': rel_app_seq
                    }
                    rel_app_text_list.append(rel_app_text)
                    rel_app_seq += 1
            dbc.insert_dict(rel_app_text, 'rel_app_text')

    def parse_brf_sum_text():
        description_element = case.find('description')
        description_text_full = etree.tostring(description_element).decode()
        brf_sum_containers = description_text_full.split("<?BRFSUM")
        if len(brf_sum_containers) > 1:
            brf_sum_text_full = "<?BRFSUM" + brf_sum_containers[1]
            text = re.sub('<.*?>|</.*?>', '', brf_sum_text_full)
            text = re.sub('[\n\t\r\f]+', ' ', text)
            text = re.sub('\s+', ' ', text)
            brf_sum_text = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'text': text
            }
            dbc.insert_dict(brf_sum_text, 'brf_sum_text')

    def parse_det_description():
        # dbc = Db()
        description_element = case.find('description')
        description_text_full = etree.tostring(description_element).decode()
        det_desc_containers = description_text_full.split("<?DETDESC")
        if len(det_desc_containers) > 1:
            det_desc_text_full = "<?DETDESC" + det_desc_containers[1]
            text = re.sub('<.*?>|</.*?>', '', det_desc_text_full)
            text = re.sub('[\n\t\r\f]+', ' ', text)
            text = re.sub('\s+', ' ', text)
            detail_desc_text = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'text': text,
                'sequence': 0
            }
            dbc.insert_dict(detail_desc_text, 'detail_desc_text')

    def parse_us_term_of_grant():
        us_term_of_grant_element = data_grant.find('us-term-of-grant')
        if us_term_of_grant_element is not None:
            us_term_of_grant = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'lapse_of_patent': get_text_or_none(us_term_of_grant_element, 'lapse-of-patent/text()'),
                'disclaimer_date': None,
                'term_disclaimer': get_text_or_none(us_term_of_grant_element, 'text/text()'),
                'term_grant': get_text_or_none(us_term_of_grant_element, 'length-of-grant/text()'),
                'term_extension': get_text_or_none(us_term_of_grant_element, 'us-term-extension/text()')
            }
            dbc.insert_dict(us_term_of_grant, 'us_term_of_grant')

    def parse_pct_data():
        publishing = data_grant.find('pct-or-regional-publishing-data')
        filling = data_grant.find('pct-or-regional-filing-data')
        pct_data_list = []
        if publishing is not None:
            date = get_text_or_none(publishing, 'document-id/date/text()')
            if date[6:] != "00":
                date = date[:4] + '-' + date[4:6] + '-' + date[6:]
            else:
                date = date[:4] + '-' + date[4:6] + '-' + '01'
            pct_data = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'rel_id': get_text_or_none(publishing, 'document-id/doc-number/text()'),
                'date': date,
                '"371_date"': None,
                'country': get_text_or_none(publishing, 'document-id/country/text()'),
                'kind': get_text_or_none(publishing, 'document-id/kind/text()'),
                'doc_type': "wo_grant",
                '"102_date"': None
            }
            pct_data_list.append(pct_data)
        if filling is not None:
            date = get_text_or_none(filling, 'document-id/date/text()')
            if date[6:] != "00":
                date = date[:4] + '-' + date[4:6] + '-' + date[6:]
            else:
                date = date[:4] + '-' + date[4:6] + '-' + '01'
            date_371 = get_text_or_none(filling, 'us-371c124-date/date/text()')
            if date_371:
                if date_371[6:] != "00":
                    date_371 = date_371[:4] + '-' + date_371[4:6] + '-' + date_371[6:]
                else:
                    date_371 = date_371[:4] + '-' + date_371[4:6] + '-' + '01'
            else:
                date_371 = None
            pct_data = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'rel_id': get_text_or_none(filling, 'document-id/doc-number/text()'),
                'date': date,
                '"371_date"': date_371,
                'country': get_text_or_none(filling, 'document-id/country/text()'),
                'kind': get_text_or_none(filling, 'document-id/kind/text()'),
                'doc_type': "pct_application",
                '"102_date"': None
            }
            pct_data_list.append(pct_data)
        dbc.insert_listdict(pct_data_list, 'pct_data')

    def parse_figures():
        figures_element = data_grant.find('figures')
        if figures_element is not None:
            figures = {
                'app_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'num_figures': int(get_text_or_none(figures_element, 'number-of-figures/text()')),
                'num_sheets': int(get_text_or_none(figures_element, 'number-of-drawing-sheets/text()'))
            }
            dbc.insert_dict(figures, 'figures')

    def parse_botanic():
        botanic_element = data_grant.find('us-botanic')
        if botanic_element is not None:
            botanic = {
                'application_id': app_id,
                'uuid': str(uuid.uuid1()),
                'patent_id': patent_id,
                'app_id': app_id,
                'latin_name': get_text_or_none(botanic_element, 'latin-name/text()'),
                'variety': get_text_or_none(botanic_element, 'variety/text()')
            }
            dbc.insert_dict(botanic, 'botanic')

    def parse_goverment_interest():
        description_element = case.find('description')
        description_text_full = etree.tostring(description_element).decode()
        gov_int_containers = description_text_full.split("<?GOVINT")
        if len(gov_int_containers) > 1:
            gov_int_text_full = "<?GOVINT" + gov_int_containers[1]
            text = re.sub('<.*?>|</.*?>', '', gov_int_text_full)
            text = re.sub('[\n\t\r\f]+', ' ', text)
            text = re.sub('\s+', ' ', text)
            goverment_interest = {
                'app_id': app_id,
                'patent_id': patent_id,
                'gi_statement': text.strip(),
            }
            dbc.insert_dict(goverment_interest, 'government_interest')

    # dbc = Db()
    start_time = time.time()

    patent = parse_patent()
    # parse_application()
    # parse_claims()
    # parse_ipcr()
    # parse_uspc()
    # parse_citations()
    # parse_assignees()
    # parse_non_inventor()
    # parse_inventors()
    # parse_agents()
    # parse_examiners()
    # parse_related_docs()
    # parse_foreign_priority()
    # parse_draw_desc_text()
    # parse_rel_app_text()
    # parse_brf_sum_text()
    # parse_det_description()
    # parse_us_term_of_grant()
    # parse_pct_data()
    # parse_figures()
    # parse_botanic()
    # parse_goverment_interest()

    with cf.ThreadPoolExecutor(max_workers=21) as executor:
        executor.submit(parse_application)
        executor.submit(parse_claims)
        executor.submit(parse_ipcr)
        executor.submit(parse_uspc)
        executor.submit(parse_citations)
        executor.submit(parse_assignees)
        executor.submit(parse_non_inventor)
        executor.submit(parse_inventors)
        executor.submit(parse_agents)
        executor.submit(parse_examiners)
        executor.submit(parse_related_docs)
        executor.submit(parse_foreign_priority)
        executor.submit(parse_draw_desc_text)
        executor.submit(parse_rel_app_text)
        executor.submit(parse_brf_sum_text)
        executor.submit(parse_det_description)
        executor.submit(parse_us_term_of_grant)
        executor.submit(parse_pct_data)
        executor.submit(parse_figures)
        executor.submit(parse_botanic)
        executor.submit(parse_goverment_interest)
    dbc.cnx.commit()

    # dbc.case_file_update_status(doc_id, 'true')
    logger.info('Inserted application %s in [%s sec]', app_id, time.time() - start_time)


def main_worker(file):
    dbc = Db()
    file_check = dbc.file_check(file)
    if file_check is None:
        xml_filename = download_file(file['url'])
        if xml_filename is not None:
            inserted_id = dbc.file_insert(file, os.path.basename(xml_filename))
            parse_file(xml_filename, inserted_id)
    elif args.parseall or file_check['status'] is None or file_check['status'] in ['new', '']:
        logger.warning('File %s is not copletly into the database. Going to process again', file_check['filename'])
        if not os.path.isfile(os.path.join(WORK_DIR, file_check['filename'])):
            xml_filename = download_file(file['url'])
        else:
            xml_filename = file_check['filename']
        if WORK_DIR not in xml_filename:
            xml_filename = os.path.join(WORK_DIR, xml_filename)
        parse_file(xml_filename, file_check['id'])
    else:
        logger.info('File %s is already inserted into database.', file_check['filename'])
        if args.parse:
            logger.info('Nothing to work. Exiting.')
            sys.exit()


logger = create_logger()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Downloads, parses and inserts to database patent applications.')
    parser.add_argument('--parse', help='Parses most recent data.', action="store_true")
    parser.add_argument('--parseall', help='Parses all the data.', action="store_true")
    parser.add_argument('--force', help='Forces to discard old data, use with --parseall command.', action="store_true")
    args = parser.parse_args()
    if args.parse or args.parseall:
        os.makedirs(os.path.dirname(WORK_DIR), exist_ok=True)
        os.makedirs(os.path.dirname(LOG_DIR), exist_ok=True)
        files_tuple = get_urls(MAIN_URL)
        # Make the following variables global
        rawlocation_list = []
        mainclassdata = {}
        subclassdata = {}
        # single-thread test
        for file in files_tuple:
            main_worker(file)
        sys.exit()
        with cf.ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(main_worker, files_tuple)
    else:
        parser.print_help()
        sys.exit()
