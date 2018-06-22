import codecs
import copy
import csv
from datetime import datetime
from html.parser import HTMLParser
import inspect
import logging
import os
from pprint import pprint
import random
import re
import string
import sys

from bs4 import BeautifulSoup as bs
# import htmlentitydefs

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
logger = logging.getLogger(__name__)


def prepare_output_dir(csv_dir):
    # Remove all files from output dir before writing
    outdir = os.listdir(csv_dir)

    for oo in outdir:
        os.remove(os.path.join(csv_dir, oo))
    # Rewrite files and write headers to them

    app_file = open(os.path.join(csv_dir, 'application.csv'), 'w')
    csvwritwer = csv.writer(app_file, delimiter='\t')
    csvwritwer.writerow(['id', 'type', 'number', 'app_id', 'country', 'date', 'abstract', 'title', 'granted',
                        'num_claims', 'filename'])
    app_file.close()

    app_assignee_file = open(os.path.join(csv_dir, 'application_assignee.csv'), 'w')
    csvwritwer = csv.writer(app_assignee_file, delimiter='\t')
    csvwritwer.writerow(['application_id', 'app_id', 'assignee_id'])
    app_assignee_file.close()

    app_inventor_file = open(os.path.join(csv_dir, 'application_inventor.csv'), 'w')
    csvwritwer = csv.writer(app_inventor_file, delimiter='\t')
    csvwritwer.writerow(['application_id', 'inventor_id'])
    app_inventor_file.close()

    assignee_file = open(os.path.join(csv_dir, 'assignee.csv'), 'w')
    csvwritwer = csv.writer(assignee_file, delimiter='\t')
    csvwritwer.writerow(['id', 'type', 'name_first', 'name_last', 'organization'])
    assignee_file.close()

    claim_file = open(os.path.join(csv_dir, 'claim.csv'), 'w')
    csvwritwer = csv.writer(claim_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'app_id', 'text', 'dependent', 'sequence'])
    claim_file.close()

    cpc_current_file = open(os.path.join(csv_dir, 'cpc_current.csv'), 'w')
    csvwritwer = csv.writer(cpc_current_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'app_id', 'section_id', 'subsection_id', 'group_id', 'subgroup_id',
                        'category', 'sequence'])
    cpc_current_file.close()

    cpc_group_file = open(os.path.join(csv_dir, 'cpc_group.csv'), 'w')
    csvwritwer = csv.writer(cpc_group_file, delimiter='\t')
    csvwritwer.writerow(['id', 'title'])
    cpc_group_file.close()

    cpc_subgroup_file = open(os.path.join(csv_dir, 'cpc_subgroup.csv'), 'w')
    csvwritwer = csv.writer(cpc_subgroup_file, delimiter='\t')
    csvwritwer.writerow(['id', 'title'])
    cpc_subgroup_file.close()

    cpc_subsection_file = open(os.path.join(csv_dir, 'cpc_subsection.csv'), 'w')
    csvwritwer = csv.writer(cpc_subsection_file, delimiter='\t')
    csvwritwer.writerow(['id', 'title'])
    cpc_subsection_file.close()

    inventor_file = open(os.path.join(csv_dir, 'inventor.csv'), 'w')
    csvwritwer = csv.writer(inventor_file, delimiter='\t')
    csvwritwer.writerow(['id', 'name_first', 'name_last'])
    inventor_file.close()

    location_file = open(os.path.join(csv_dir, 'location.csv'), 'w')
    csvwritwer = csv.writer(location_file, delimiter='\t')
    csvwritwer.writerow(['id', 'city', 'state', 'country', 'latitude', 'longitude'])
    location_file.close()

    location_assignee_file = open(os.path.join(csv_dir, 'location_assignee.csv'), 'w')
    csvwritwer = csv.writer(location_assignee_file, delimiter='\t')
    csvwritwer.writerow(['location_id', 'assignee_id'])
    location_assignee_file.close()

    location_inventor_file = open(os.path.join(csv_dir, 'location_inventor.csv'), 'w')
    csvwritwer = csv.writer(location_inventor_file, delimiter='\t')
    csvwritwer.writerow(['location_id', 'inventor_id'])
    location_inventor_file.close()

    mainclass_file = open(os.path.join(csv_dir, 'mainclass.csv'), 'w')
    csvwritwer = csv.writer(mainclass_file, delimiter='\t')
    csvwritwer.writerow(['id'])
    mainclass_file.close()

    mainclass_current_file = open(os.path.join(csv_dir, 'mainclass_current.csv'), 'w')
    csvwritwer = csv.writer(mainclass_current_file, delimiter='\t')
    csvwritwer.writerow(['id', 'title'])
    mainclass_current_file.close()

    rawassignee_file = open(os.path.join(csv_dir, 'rawassignee.csv'), 'w')
    csvwritwer = csv.writer(rawassignee_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'assignee_id', 'rawlocation_id', 'type', 'name_first', 'name_last',
                        'organization', 'city', 'state', 'country', 'sequence'])
    rawassignee_file.close()

    rawinventor_file = open(os.path.join(csv_dir, 'rawinventor.csv'), 'w')
    csvwritwer = csv.writer(rawinventor_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'app_id', 'inventor_id', 'rawlocation_id', 'name_first',
                        'name_last', 'city', 'state', 'country', 'sequence'])
    rawinventor_file.close()

    rawlocation_file = open(os.path.join(csv_dir, 'rawlocation.csv'), 'w')
    csvwritwer = csv.writer(rawlocation_file, delimiter='\t')
    csvwritwer.writerow(['id', 'location_id', 'city', 'state', 'country', 'country_transformed'])
    rawlocation_file.close()

    subclass_file = open(os.path.join(csv_dir, 'subclass.csv'), 'w')
    csvwritwer = csv.writer(subclass_file, delimiter='\t')
    csvwritwer.writerow(['id'])
    subclass_file.close()

    subclass_current_file = open(os.path.join(csv_dir, 'subclass_current.csv'), 'w')
    csvwritwer = csv.writer(subclass_current_file, delimiter='\t')
    csvwritwer.writerow(['id', 'title'])
    subclass_current_file.close()

    temporary_update_file = open(os.path.join(csv_dir, 'temporary_update.csv'), 'w')
    csvwritwer = csv.writer(temporary_update_file, delimiter='\t')
    csvwritwer.writerow(['pk', 'update'])
    temporary_update_file.close()

    uspc_file = open(os.path.join(csv_dir, 'uspc.csv'), 'w')
    csvwritwer = csv.writer(uspc_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'app_id', 'mainclass_id', 'subclass_id', 'sequence'])
    uspc_file.close()

    uspc_current_file = open(os.path.join(csv_dir, 'uspc_current.csv'), 'w')
    csvwritwer = csv.writer(uspc_current_file, delimiter='\t')
    csvwritwer.writerow(['uuid', 'application_id', 'app_id', 'mainclass_id', 'subclass_id', 'sequence'])
    uspc_current_file.close()


def parse_patents(xml_dir, csv_dir):
    _char = re.compile(r'&(\w+?);')

    # Generate some extra HTML entities
    # defs=htmlentitydefs.entitydefs
    defs = {}
    defs['apos'] = "'"
    # find the entities file
    try:
        htmlentities_path = os.path.join(parentdir, 'htmlentities.txt')
        entities = open(htmlentities_path, 'rb').read().decode().split('\n')
    except Exception as e:  # slighly hacky for running separately to debug
        logger.warning('Cannot open htmlentities')
        raise
    for e in entities:
        try:
            first = re.sub('\s+|\"|;|&', '', e[3:15])
            second = re.sub('\s+|\"|;|&', '', e[15:24])
            define = re.search("(?<=\s\s\').*?$", e).group()
            defs[first] = define[:-1].encode('utf-8')
            defs[second] = define[:-1].encode('utf-8')
        except Exception:
            pass

    def _char_unescape(m, defs=defs):
        try:
            return defs[m.group(1)].encode('utf-8', 'ignore')
        except Exception:
            return m.group()

    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    xml_dir += '/'
    csv_dir += '/'
    diri = os.listdir(xml_dir)
    diri = [d for d in diri if re.search('XML', d, re.I)]
    logger.info('List of XML files is %s', diri)

    # Emty content of CSV_DIR and create new empty csv files
    prepare_output_dir(csv_dir)

    # Initiate HTML Parser for unescape characters
    h = HTMLParser()

    loggroups = ["publication-reference", "application-reference", "us-application-series-code", "classification-ipcr",
                 "classification-cpc", "invention-title", "us-related-documents", "us-applicants", "inventors",
                 "abstract", "claims", "pct-or-regional-filing-data", "assignees", "classification-national", 
                 "priority-claim"]

    numi = 0
    
    # Rawlocation, mainclass and subclass should write after all else is done to prevent duplicate values
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}

    # diri = [d for d in diri if d.startswith("ipg" + str(year))]
    for d in diri:
        logger.info('Starting with file %s', d)
        infile = open(xml_dir+d, 'rb').read().decode('utf-8', 'ignore').replace('&angst', '&aring')
        # infile = infile.encode('utf-8', 'ignore')
        infile = _char.sub(_char_unescape, infile)
        # infile = h.unescape(infile).encode('utf-8')
        infile = infile.split('<!DOCTYPE')
        del infile[0]

        numi += len(infile)

        for i in infile:
            avail_fields = {}
            # parser for logical groups
            for j in loggroups:
                list_for_group = i.split("\n<"+j)
                # this only processes the groups that split it into two exactly (meaning it exists)
                # and also not the logical groups that appear multiple times
                # there can be multiple agents, for example, but they all show up in on "agents" group
                # this only parses the groups that occur once (so not when it doesn't exist)
                if len(list_for_group) == 2:
                    results = list_for_group[1].split(j+">")
                    avail_fields[j] = results[0]
                if len(list_for_group) > 2:
                    # i think too many classification-national files are being parsed
                    items = []
                    for k in list_for_group[1:]:  # drop the first group
                        results = k.split(j+">")
                        items.append(results[0])
                    avail_fields[j] = items

            # Create containers based on existing Berkeley DB schema 
            # (not all are currently used - possible compatibility issues)
            application = {
                'filename': d
            }

            application_assignee = {}
            application_inventor = {}
            assignee = {}
            claim = {}
            cpc_current = {}
            cpc_group = {}
            cpc_subgroup = {}
            cpc_subsection = {}
            inventor = {}
            location = {}
            location_assignee = {}
            location_inventor = {}
            mainclass = {}
            mainclass_current = {}
            rawassignee = {}
            rawinventor = {}
            rawlocation = {}
            subclass = {}
            subclass_current = {}
            temporary_update = {}
            uspc = {}
            uspc_current = {}

            #  PARSERS FOR LOGICAL GROUPS
            try:
                publication = avail_fields['publication-reference'].split("\n")
                for line in publication:
                    if line.startswith("<doc-number"):
                        docno = re.search('<doc-number>(.*?)</doc-number>', line).group(1)
                    if line.startswith("<kind"):
                        patkind = re.search('<kind>(.*?)</kind>', line).group(1)
                    if line.startswith("<country"):
                        patcountry = re.search('<country>(.*?)</country>', line).group(1)
                    if line.startswith("<date"):
                        issdate = re.search('<date>(.*?)</date>', line).group(1)
                        if issdate[6:] != "00":
                            issdate = issdate[:4] + '-' + issdate[4:6] + '-' + issdate[6:]
                        else:
                            issdate = issdate[:4] + '-' + issdate[4:6] + '-' + '01'
                            year = issdate[:4]
                        application['date'] = datetime.strptime(issdate, '%Y-%m-%d').date()

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
                # patent_id = docno
                application['number'] = docno
            except Exception as e:
                print(type(e), str(e))
                logger.warning('Failed at publication-reference for %s', docno)
                logger.exception('message')

            try:
                abst = None
                for_abst = avail_fields['abstract']
                split_lines = for_abst.split("\n")
                abst = re.search('">(.*?)</p', split_lines[1]).group(1)
                application['abstract'] = abst
            except Exception as e:
                print(type(e), str(e))

            # try:
            title = None
            if 'invention-title' in avail_fields:
                title = re.search('>(.*?)<', avail_fields["invention-title"]).group(1)
                if title == '':
                    text = avail_fields['invention-title']
                    title = text[text.find('>')+1:text.rfind('<')]
                application['title'] = title

            try:
                series_code = None
                app_series_code = avail_fields['us-application-series-code']
                series_code = re.search(">(.*?)</", app_series_code).group(1)
            except Exception as e:
                print(type(e), str(e))

            try:
                application_list = avail_fields['application-reference'].split("\n")
                apptype = None
                for line in application_list:
                    if line.startswith("<doc-number"):
                        appnum = re.search('<doc-number>(.*?)</doc-number>', line).group(1)
                        app_id = appnum
                        application['app_id'] = app_id
                    if line.startswith("<country"):
                        appcountry = re.search('<country>(.*?)</country>', line).group(1)
                        application['country'] = appcountry
                    if line.startswith("<date"):
                        appdate = re.search('<date>(.*?)</date>', line).group(1)
                        if appdate[6:] != "00":
                            appdate = appdate[:4]+'-' + appdate[4:6]+'-' + appdate[6:]
                        else:
                            appdate = appdate[:4]+'-'+appdate[4:6]+'-'+'01'
                            year = appdate[:4]
                    if line.startswith(" appl-type"):
                        apptype = re.search('"(.*?)"', line).group(1)
                        application['type'] = apptype
                # modeled on the 2005 approach because apptype can be none in 2005, making the 2002 approach not work
                # but using the full application number as done in 2005
                application['id'] = issdate[:4] + "/" + application['number']
            except Exception:
                print(type(e), str(e))
                raise

            claims_list = []
            try:
                numclaims = 0
                claimsdata = re.search('<claims.*?>(.*?)</claims>', i, re.DOTALL).group(1)
                claim_number = re.finditer('<claim id(.*?)>', claimsdata, re.DOTALL)
                claims_iter = re.finditer('<claim.*?>(.*?)</claim>', claimsdata, re.DOTALL)

                claim_info = []
                claim_num_info = []
                for i, claim in enumerate(claims_iter):
                    claim = claim.group(1)
                    this_claim = []
                    try:
                        dependent = re.search('<claim-ref idref="CLM-(\d+)">', claim).group(1)
                        dependent = int(dependent)
                        # this_claim.append(dependent)
                    except Exception:
                        dependent = None
                    text = re.sub('<.*?>|</.*?>', '', claim)
                    text = re.sub('[\n\t\r\f]+', '', text)
                    text = re.sub('^\d+\.\s+', '', text)
                    text = re.sub('\s+', ' ', text)
                    sequence = i+1  # claims are 1-indexed
                    this_claim.append(text)
                    # this_claim.append(sequence)
                    this_claim.append(dependent)
                    claim_info.append(this_claim)

                for i, claim_num in enumerate(claim_number):
                    claim_num = claim_num.group(1)
                    clnum = re.search('num="(.*?)"', claim_num).group(1)
                    claim_num_info.append(clnum)
                numclaims = len(claim_num_info)

                for i in range(len(claim_info)):
                    claim = {
                        'uuid': id_generator(),
                        'application_id': application['id'],
                        'app_id': application['app_id'],
                        'text': claim_info[i][0],
                        'dependent': claim_info[i][1],
                        'sequence': claim_num_info[i]
                    }
                    # claims[app_id] = [id_generator(), patent_id, claim_info[i][0],
                                    #   claim_info[i][1], str(claim_num_info[i]), exemplary]
                    claims_list.append(claim)
            except Exception as e:
                print(type(e), str(e))
                raise
            application['num_claims'] = numclaims

            # specially check this
            cpc_current_list = []
            try:
                if 'classification-ipcr' in avail_fields:
                    ipcr_classification = avail_fields["classification-ipcr"]
                else:
                    ipcr_classification = avail_fields["classification-ipc"]
                # makes a single ipc classification into a list so it processes properly
                if type(ipcr_classification) is str:
                    ipcr_classification = [ipcr_classification]
                num = 0
                for j in ipcr_classification:
                    class_level = None
                    section = None
                    mainclass = None
                    subclass = None
                    group = None
                    subgroup = None
                    symbol_position = None
                    classification_value = None
                    classification_status = None
                    classification_source = None
                    action_date = None
                    ipcrversion = None
                    ipcr_fields = j.split("\n")
                    for line in ipcr_fields:
                        if line.startswith("<classification-level"):
                            class_level = re.search('<classification-level>(.*?)</classification-level>', line).group(1)
                        if line.startswith("<section"):
                            section = re.search('<section>(.*?)</section>', line).group(1)
                        if line.startswith("<class>"):
                            mainclass = re.search('<class>(.*?)</class>', line).group(1)
                        if line.startswith("<subclass"):
                            subclass = re.search('<subclass>(.*?)</subclass>', line).group(1)
                        if line.startswith("<main-group"):
                            group = re.search('<main-group>(.*?)</main-group>', line).group(1)
                        if line.startswith("<subgroup"):
                            subgroup = re.search('<subgroup>(.*?)</subgroup>', line).group(1)
                        if line.startswith("<ipc-version-indicator"):
                            ipcrversion = re.search('<date>(.*?)</date>', line).group(1)
                            if ipcrversion[6:] != "00":
                                ipcrversion = ipcrversion[:4]+'-' + ipcrversion[4:6]+ '-' + ipcrversion[6:]
                            else:
                                ipcrversion = ipcrversion[:4] + '-' + ipcrversion[4:6]+'-'+'01'
                        if line.startswith("<symbol-position"):
                            symbol_position = re.search('<symbol-position>(.*?)</symbol-position>', line).group(1)
                        if line.startswith("<classification-value"):
                            classification_value = re.search(
                                '<classification-value>(.*?)</classification-value>', line).group(1)
                        if line.startswith("<classification-status"):
                            classification_status = re.search(
                                '<classification-status>(.*?)</classification-status>', line).group(1)
                        if line.startswith("<classification-data-source"):
                            classification_source = re.search(
                                '<classification-data-source>(.*?)</classification-data-source>', line).group(1)
                        if line.startswith("<action-date>"):
                            action_date = re.search('<date>(.*?)</date>', line).group(1)
                            if action_date[6:] != "00":
                                action_date = action_date[:4]+'-' + action_date[4:6]+'-'+action_date[6:]
                            else:
                                action_date = action_date[:4] + '-'+action_date[4:6]+'-'+'01'
                    # if all the valu[es are "NULL", set will have len(1) and we can ignore it. This is bad
                    values = set([section, mainclass, subclass, group, subgroup])
                    if classification_value == 'I':
                        cpc_category = 'invention'
                    elif classification_value == 'A':
                        cpc_category = 'addition'
                    else:
                        cpc_category = None
                    if len(values) > 1:  # get rid of the situation in which there is no data
                        cpc_current = {
                            'uuid': id_generator(),
                            'application_id': application['id'],
                            'app_id': application['app_id'],
                            'section_id': section,
                            'subsection_id': section + mainclass,
                            'group_id': section + mainclass + subclass,
                            'subgroup_id': section + mainclass + subclass + group,
                            'category': cpc_category,
                            'sequence': str(num),
                        }
                        cpc_current_list.append(cpc_current)
                        # ipcr[app_id] = [id_generator(), patent_id, class_level, section, mainclass, subclass, group, 
                                        # subgroup, symbol_position, classification_value, classification_status, 
                                        # classification_source, action_date, ipcrversion, str(num)]
                    num += 1
            except Exception as e:
                print(type(e), str(e))

            # data = {'class': main[0][:3].replace(' ', ''),
                # 'subclass': crossrefsub}

            if 'classification-national' in avail_fields:
                national = avail_fields["classification-national"]
                national = national[0]
                national = national.split("\n")
                n = 0
                for line in national:
                    if line.startswith('<main-classification'):
                        main = re.search('<main-classification>(.*?)</main-classification', line).group(1)
                        crossrefsub = main[3:].replace(" ", "")
                        if len(crossrefsub) > 3 and re.search('^[A-Z]', crossrefsub[3:]) is None:
                            crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
                        crossrefsub = re.sub('^0+', '', crossrefsub)
                        if re.search('[A-Z]{3}', crossrefsub[:3]):
                            crossrefsub = crossrefsub.replace(".", "")
                        main = main[:3].replace(" ", "")
                        sub = crossrefsub
                        mainclassdata[main] = [main]
                        subclassdata[sub] = [sub]
                    elif line.startswith("<further-classification"):
                        further_class = re.search('<further-classification>(.*?)</further-classification', line).group(1)
                        further_sub_class = further_class[3:].replace(" ", "")
                        if len(further_sub_class) > 3 and re.search('^[A-Z]', further_sub_class[3:]) is None:
                            further_sub_class = further_sub_class[:3] + '.'+further_sub_class[3:]
                        further_sub_class = re.sub('^0+', '', further_sub_class)
                        if re.search('[A-Z]{3}', further_sub_class[:3]):
                            further_sub_class = further_sub_class.replace(".", "")
                        further_class = further_class[:3].replace(" ", "")
                        mainclassdata[further_class] = [further_class]
                        if further_sub_class != "":
                            uspc[app_id] = [id_generator(), patent_id, further_class,
                                            further_class+'/'+further_sub_class, str(n)]
                            subclassdata[further_class+'/'+further_sub_class] = [
                                further_class+'/'+further_sub_class]
                            n += 1

            # I think it may be the patent cit/other cit problem
            if ("references-cited" in avail_fields) | ('us-references-cited' in avail_fields):
                if "references-cited" in avail_fields:
                    citation = re.split("<citation", avail_fields['references-cited'])
                # in 2021 schema changed to us-references-cited instead of references-cited
                else:
                    citation = re.split("<us-citation", avail_fields['us-references-cited'])
                otherseq = 0
                forpatseq = 0
                uspatseq = 0
                appseq = 0
                for i in citation:
                    # print citation
                    refpatnum = 'NULL'
                    citpatname = 'NULL'
                    citdate = 'NULL'
                    citkind = 'NULL'
                    citcountry = 'US'
                    citdocno = 'NULL'
                    citcategory = "NULL"
                    text = "NULL"
                    app_flag = False
                    ref_class = "NULL"

                    # print
                    to_process = i.split('\n')
                    for line in to_process:
                        if line.startswith("<doc-number"):
                            citdocno = re.search('<doc-number>(.*?)</doc-number>', line).group(1)
                            # figure out if this is a citation to an application
                            if re.match(r'^[A-Z]*\d+$', citdocno):
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
                        try:
                            if line.startswith("<kind"):
                                citkind = re.search('<kind>(.*?)</kind>', line).group(1)
                            if line.startswith("<category"):
                                citcategory = re.search('<category>(.*?)</category>', line).group(1)
                            if line.startswith("<country"):
                                citcountry = re.search('<country>(.*?)</country>', line).group(1)
                            if line.startswith("<name"):
                                name = re.search("<name>(.*?)</name>", line).group(1)
                            if line.startswith("<classification-national"):
                                ref_class = re.search("<main-classification>(.*?)</main-classification", line).group(1)
                            if line.startswith("<date"):
                                citdate = re.search('<date>(.*?)</date>', line).group(1)
                                if citdate[6:] != "00":
                                    citdate = citdate[:4]+'-' + citdate[4:6]+'-'+citdate[6:]
                                else:
                                    citdate = citdate[:4] + '-'+citdate[4:6]+'-'+'01'
                                    year = citdate[:4]
                            if line.startswith("<othercit"):
                                text = re.search("<othercit>(.*?)</othercit>", line).group(1)
                        except Exception:
                            print("Problem with other variables")
                    if citcountry == "US":
                        if citdocno != "NULL" and not app_flag:
                            uspatentcitation[app_id] = [id_generator(), patent_id, citdocno, citdate,
                                                        name, citkind, citcountry, citcategory, str(uspatseq), ref_class]
                            uspatseq += 1
                        if citdocno != 'NULL' and app_flag:
                            usappcitation[app_id] = [id_generator(), patent_id, citdocno, citdate, name,
                                                     citkind, citdocno, citcountry, citcategory, str(appseq)]
                            appseq += 1
                    elif citdocno != "NULL":
                        foreigncitation[app_id] = [id_generator(), patent_id, citdate, citdocno,
                                                   citcountry, citcategory, str(forpatseq)]
                        forpatseq += 1
                    if text != "NULL":
                        otherreference[app_id] = [id_generator(), patent_id, text, str(otherseq)]
                        otherseq += 1

            try:
                if 'assignees' in avail_fields:
                    # splits fields if there are multiple assignees
                    assignees = avail_fields['assignees'].split("</assignee")
                    # exclude the last chunk which is a waste line from after the
                    assignees = assignees[:-1]
                    # list_of_assignee_info = []
                    for i in range(len(assignees)):
                        assgorg = None
                        assgfname = None
                        assglname = None
                        assgtype = None
                        assgcountry = 'US'
                        assgstate = None
                        assgcity = None
                        one_assignee = assignees[i].split("\n")
                        for line in one_assignee:
                            if line.startswith("<orgname"):
                                assgorg = re.search('<orgname>(.*?)</orgname>', line).group(1)
                            if line.startswith("<firstname"):
                                assgfname = re.search('<firstname>(.*?)</firstname>', line).group(1)
                            if line.startswith("<lastname"):
                                assglname = re.search('<lastname>(.*?)</lastname>', line).group(1)
                            # handle differnt spelling of tags
                            if line.startswith("<last-name"):
                                assglname = re.search('<last-name>(.*?)</last-name>', line).group(1)
                            if line.startswith("<first-name"):
                                assgfname = re.search('<first-name>(.*?)</first-name>', line).group(1)
                            if line.startswith("<role"):
                                assgtype = re.search('<role>(.*?)</role>', line).group(1)
                                assgtype = assgtype.lstrip("0")
                            if line.startswith("<country"):
                                assgcountry = re.search('<country>(.*?)</country>', line).group(1)
                            if line.startswith("<state"):
                                assgstate = re.search('<state>(.*?)</state>', line).group(1)
                            if line.startswith("<city"):
                                assgcity = re.search('<city>(.*?)</city>', line).group(1)
                        loc_idd = id_generator()
                        rawlocation[loc_idd] = [
                            None, assgcity, assgstate, assgcountry]
                        rawassignee[app_id] = [id_generator(), patent_id, None, loc_idd, assgtype,
                                               assgfname, assglname, assgorg, str(i)]
                else:
                    pass

            except Exception:
                print(patent_id, "problem with assignee")

            try:
                if 'applicants' in avail_fields:
                    applicant_list = re.split("</applicant>", avail_fields['applicants'])
                    # gets rid of extra last line
                    applicant_list = applicant_list[:-1]
                    # print applicant_list
                else:
                    applicant_list = re.split("</us-applicant>", avail_fields['us-applicants'])
                    # gets rid of extra last line
                    applicant_list = applicant_list[:-1]
                inventor_seq = 0
                for person in applicant_list:
                    designation = "NULL"
                    earlier_applicant_type = "NULL"
                    later_applicant_type = "NULL"
                    sequence = "NULL"
                    last_name = "NULL"
                    first_name = 'NULL'
                    orgname = "NULL"
                    street = 'NULL'
                    city = 'NULL'
                    state = "NULL"
                    country = 'NULL'
                    nationality = 'NULL'
                    residence = 'NULL'
                    applicant_lines = person.split("\n")
                    for line in applicant_lines:
                        if line.startswith('<applicant'):
                            sequence = re.search('sequence="(.*?)"', line).group(1)
                            earlier_applicant_type = re.search('app-type="(.*?)"', line).group(1)
                            designation = re.search('designation="(.*?)"', line).group(1)
                        if line.startswith('<us-applicant'):
                            sequence = re.search('sequence="(.*?)"', line).group(1)
                            designation = re.search('designation="(.*?)"', line).group(1)
                            try:
                                later_applicant_type = re.search('applicant-authority-category="(.*?)"', line).group(1)
                            except Exception:
                                pass
                        if line.startswith("<orgname"):
                            orgname = re.search('<orgname>(.*?)</orgname>', line).group(1)
                        if line.startswith("<first-name"):
                            first_name = re.search('<first-name>(.*?)</first-name>', line).group(1)
                        if line.startswith("<last-name"):
                            last_name = re.search('<last-name>(.*?)</last-name>', line).group(1)
                        if line.startswith('<street'):
                            street = re.search('<street>(.*?)</street>', line).group(1)
                        if line.startswith('<city'):
                            city = re.search('<city>(.*?)</city>', line).group(1)
                        if line.startswith("<state"):
                            state = re.search('<state>(.*?)</state>', line).group(1)
                        if line.startswith('<country'):
                            country = re.search('<country>(.*?)</country>', line).group(1)
                    try:  # nationality only in earlier years
                        for_nat = person.split("nationality")
                        nation = for_nat[1].split("\n")
                        for line in nation:
                            if line.startswith("<country"):
                                nation = re.search('<country>(.*?)</country>', line).group(1)
                    except Exception:
                        pass
                    for_res = person.split("residence")
                    res = for_res[1].split("\n")
                    for line in res:
                        if line.startswith("<country"):
                            residence = re.search('<country>(.*?)</country>', line).group(1)
                    # this get us the non-inventor applicants in 2013+. Inventor applicants are in applicant and also in inventor.
                    non_inventor_app_types = [
                        'legal-representative', 'party-of-interest', 'obligated-assignee', 'assignee']
                    if later_applicant_type in non_inventor_app_types:
                        loc_idd = id_generator()
                        rawlocation[loc_idd] = [None, city, state, country]
                        non_inventor_applicant[app_id] = [id_generator(), patent_id, loc_idd, last_name,
                                                          first_name, orgname, sequence, designation, later_applicant_type]
                    # this gets us the inventors from 2005-2012
                    if earlier_applicant_type == "applicant-inventor":
                        loc_idd = id_generator()
                        rawlocation[loc_idd] = [None, city, state, country]
                        rawinventor[app_id] = [id_generator(), patent_id, None, loc_idd, first_name,
                                               last_name, str(inventor_seq), rule_47]
                        inventor_seq += 1
                    if (earlier_applicant_type != "applicant-inventor") and earlier_applicant_type != "NULL":
                        loc_idd = id_generator()
                        rawlocation[loc_idd] = [None, city, state, country]
                        non_inventor_applicant[app_id] = [id_generator(), patent_id, loc_idd, last_name,
                                                          first_name, orgname, sequence, designation, earlier_applicant_type]
            except Exception:
                pass

            try:
                # 2005-2013 (inclusive) ame of the inventor is only stored in inventor if the inventor is not also the applicant in earlier year
                # mostly thi is because the inventor is dead
                # so inventors has dead /incapacitated invenntors adn "applicant " has live

                # 2013 on name of the inventor is stored in the inventors block always
                # split on live or deceased inventor
                inventors = re.split("<inventor sequence", avail_fields['inventors'])
                # inventors = inventors[1:] #exclude the last chunk which is a waste line from after the inventor
                for i in range(len(inventors)):
                    fname = "NULL"
                    lname = "NULL"
                    invtcountry = 'US'
                    invtstate = "NULL"
                    invtcity = "NULL"
                    invtzip = "NULL"
                    one_inventor = inventors[i].split("\n")
                    for line in one_inventor:
                        if line.startswith("<first-name"):
                            fname = re.search('<first-name>(.*?)</first-name>', line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search('<last-name>(.*?)</last-name>', line).group(1)
                        if line.startswith("<zip"):
                            invtzip = re.search('<zip>(.*?)</zip>', line).group(1)
                        if line.startswith("<country"):
                            invtcountry = re.search('<country>(.*?)</country>', line).group(1)
                        if line.startswith("<state"):
                            invtstate = re.search('<state>(.*?)</state>', line).group(1)
                        if line.startswith("<city"):
                            invtcity = re.search('<city>(.*?)</city>', line).group(1)
                    if fname == "NULL" and lname == "NULL":
                        pass
                    else:
                        loc_idd = id_generator()
                        rawlocation[loc_idd] = [
                            None, invtcity, invtstate, invtcountry]
                        rawinventor[app_id] = [id_generator(), patent_id, None, loc_idd,
                                               fname, lname, str(i-1), rule_47]
            except Exception:
                pass

            try:
                agent = re.split("</agent", avail_fields['agents'])
                agent = agent[:-1]
                for i in range(len(agent)):
                    fname = ""
                    lname = ""
                    lawcountry = ''
                    laworg = ""
                    one_agent = agent[i].split("\n")
                    for line in one_agent:
                        if line.startswith("<first-name"):
                            fname = re.search('<first-name>(.*?)</first-name>', line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search('<last-name>(.*?)</last-name>', line).group(1)
                        if line.startswith("<country"):
                            lawcountry = re.search('<country>(.*?)</country>', line).group(1)
                        if line.startswith("<orgname"):
                            laworg = re.search('<orgname>(.*?)</orgname>', line).group(1)
                        if line.startswith("<agent sequence"):
                            rep_type = re.search('rep-type=\"(.*?)\"', line).group(1)
                    rawlawyer[app_id] = [id_generator(), "", patent_id, fname, lname, laworg, lawcountry, str(i)]
            except Exception:
                pass

            if 'us-related-documents' in avail_fields:
                related_docs = avail_fields['us-related-documents']
                possible_doc_type = ["</division>", '</continuation>', '</continuation-in-part>', '</continuing-reissue>',
                                     '</us-divisional-reissue>', '</reexamination>', '</substitution>', '</us-provisional-application>', '</utility-model-basis>',
                                     '</reissue>', '</related-publication>', '</correction>',
                                     '</us-provisional-application>', '</us-reexamination-reissue-merger>']

                def iter_list_splitter(list_to_split, seperators):
                    for j in seperators:
                        new_list = []
                        for i in list_to_split:
                            split_i = i.split(j)
                            new_list += split_i
                        list_to_split = copy.deepcopy(new_list)
                    return list_to_split
                split_docs = iter_list_splitter(
                    [related_docs], possible_doc_type)
                possible_relations = ['<parent-doc>', '<parent-grant-document>', '<parent-pct-document>', '<child-doc>']
                rel_seq = 0
                kind = None
                for i in split_docs:
                    if i != '\r\n</':  # sometimes this extra piece exists so we should get rid of it
                        doc_info = i.split("\n")
                        doc_type = doc_info[1][1:-2]  # cuts off the < adn >
                        if ((doc_type == "us-provisional-application") | (doc_type == "related-publication")):
                            doc_info = i.split("\n")
                            # cuts off the < adn >
                            doc_type = doc_info[1][1:-2]
                            for line in doc_info:
                                if line.startswith("<doc-number"):
                                    reldocno = re.search('<doc-number>(.*?)</doc-number>', line).group(1)
                                if line.startswith("<kind"):
                                    kind = re.search('<kind>(.*?)</kind>', line).group(1)
                                if line.startswith("<country"):
                                    relcountry = re.search('<country>(.*?)</country>', line).group(1)
                                try:
                                    if line.startswith("<date"):
                                        reldate = re.search('<date>(.*?)</date>', line).group(1)
                                        if reldate[6:] != "00":
                                            reldate = reldate[:4]+'-' + reldate[4:6]+'-'+reldate[6:]
                                        else:
                                            reldate = reldate[:4] + '-'+reldate[4:6]+'-'+'01'
                                            year = reldate[:4]
                                except Exception:
                                    print("Missing date on usreldoc")
                                    reldate = "0000-00-00"
                            usreldoc[app_id] = [id_generator(), patent_id, doc_type, "NULL", reldocno, relcountry, reldate, "NULL", rel_seq, kind]
                            # usrel.writerow(['uuid', 'patent_id', 'doc_type',  'relkind', 'reldocno', 'relcountry', 'reldate',  'parent_status', 'rel_seq','kind'])
                            rel_seq += 1
                        else:
                            split_by_relation = iter_list_splitter([i], possible_relations)
                            # the first item in the list is not a document
                            for j in split_by_relation[1:]:
                                parent_or_child = j.split("\n")
                                reltype = None
                                if re.search("</child-doc", j):
                                    reltype = "child document"
                                elif re.search("</parent-grant-document", j):
                                    reltype = "parent grant document"
                                elif re.search("</parent-pct-document", j):
                                    reltype = "parent pct document"
                                else:
                                    # because of how the text is split parent document doesn't in the doc info for the parent document
                                    reltype = "parent document"
                                reldocno = None
                                kind = None
                                relcountry = None
                                reldate = None
                                relparentstatus = None
                                for line in parent_or_child:
                                    if line.startswith("<doc-number"):
                                        reldocno = re.search('<doc-number>(.*?)</doc-number>', line).group(1)
                                    if line.startswith("<kind"):
                                        kind = re.search('<kind>(.*?)</kind>', line).group(1)
                                    if line.startswith("<country"):
                                        relcountry = re.search('<country>(.*?)</country>', line).group(1)
                                    try:
                                        if line.startswith("<date"):
                                            reldate = re.search('<date>(.*?)</date>', line).group(1)
                                            if reldate[6:] != "00":
                                                reldate = reldate[:4]+'-' + reldate[4:6] + '-'+reldate[6:]
                                            else:
                                                reldate = reldate[:4] + '-'+reldate[4:6]+'-'+'01'
                                                year = reldate[:4]
                                    except Exception:
                                        reldate = "0000-00-00"
                                        print("Missing date on reldoc")
                                    if line.startswith("<parent-status"):
                                        relparentstatus = re.search('<parent-status>(.*?)</parent-status', line).group(1)
                                usreldoc[app_id] = [id_generator(), patent_id, doc_type, reltype, reldocno,
                                                    relcountry, reldate,  relparentstatus, rel_seq, kind]
                                rel_seq += 1

            try:
                examiners = avail_fields["examiners"].split("<assistant-examiner")
                department = "Null"
                for i in range(len(examiners)):
                    list_of_examiner = examiners[i].split("\n")
                    fname = "Null"
                    lname = "Null"
                    for line in list_of_examiner:
                        if line.startswith("<first-name"):
                            fname = re.search("<first-name>(.*?)</first-name>", line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search("<last-name>(.*?)</last-name>", line).group(1)
                        if line.startswith("<department"):
                            department = re.search("<department>(.*?)</department>", line).group(1)
                    if i == 0:  # the first examiner is the primary examiner
                        examiner[app_id] = [id_generator(), docno, fname, lname, "primary", department]
                    else:
                        examiner[app_id] = [id_generator(), docno, fname, lname, "assistant", department]
            except Exception:
                pass

            if "priority-claim" in avail_fields:
                # first entry is a waste
                priority = avail_fields["priority-claim"][1:]
                # print priority
                sequence = 0
                for i in priority:
                    priority_id = "NULL"
                    priority_requested = "NULL"
                    priority_claim = i.split("\n")
                    for line in priority_claim:
                        if line.startswith("<country"):
                            country = re.search("<country>(.*?)</country>", line).group(1)
                        if line.startswith("<doc-number"):
                            app_num = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                        if line.startswith("<date"):
                            app_date = re.search("<date>(.*?)</date>", line).group(1)
                            if app_date[6:] != "00":
                                app_date = app_date[:4]+'-' + app_date[4:6]+'-' + app_date[6:]
                            else:
                                app_date = app_date[:4] + '-'+appd_ate[4:6]+'-'+'01'
                        if line.startswith(" sequence"):
                            kind = re.search('kind="(.*?)"', line).group(1)
                        if line.startswith("<id"):
                            priority_id = re.search("<id>(>*?)<id>", line).group(1)
                        if line.startswith("<priority-doc-requested"):
                            priority_requested = re.search("<priority-doc-requested>(.*?)</priority-doc-requested", line).group(1)
                    # priority_id and priority_requested cant be found
                    for_priority[app_id] = [id_generator(), patent_id, sequence, kind, app_num, app_date, country]
                    sequence += 1

            # if "description" in avail_fields:
                #print avail_fields["description"]

            try:
                rel_app = avail_fields["RELAPP"].split("\n")
                rel_app_seq = 0
                text_field = ""
                for line in rel_app:
                    if line.startswith("<heading"):
                        rel_app_seq += 1
                        heading = re.search(">(.*?)<", line).group(1)
                        text_field += heading + " "
                        #rel_app_text[id_generator()] = [patent_id,"heading", heading, rel_app_seq]
                    if line.startswith("<p"):
                        rel_app_seq += 1
                        text = re.search(">(.*?)</p>", line).group(1)
                        text_field += text + " "
                rel_app_text[app_id] = [id_generator(), patent_id, text_field]
            except Exception:
                pass

            try:
                draw_seq = 0
                draw_des = avail_fields['description-of-drawings'].split("\n")
                #print draw_des
                draw_text = ''
                for line in draw_des:
                    if line.startswith("<p id"):
                        draw_seq += 1
                        # this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        draw_desc, cut, end = text.partition("</p")
                        if "<" in draw_desc or ">" in draw_desc:
                            soup = bs(draw_desc, "lxml")
                            text = soup.get_text()
                        else:
                            text = draw_desc
                        # text = [piece.encode('utf-8','ignore') for piece in text]
                        # text = "".join(text)
                        # if not (text.strip() in ["BRIEF DESCRIPTION OF THE DRAWINGS", "BRIEF DESCRIPTION OF THE DRAWING", "BRIEF DESCRIPTION OF THE DRAWING"]):
                        if (not text.isupper()) | (any(char.isdigit() for char in text)):
                            draw_desc_text[app_id] = [id_generator(), patent_id, text, draw_seq]
                        else:
                            pass  # skipping the brief description heading
                    if line.startswith("<heading"):
                        draw_seq += 1

                        heading = re.search(">(.*?)<", line).group(1)
                        # draw_text += " " + heading
                        if (not desc.isupper()) | (any(char.isdigit() for char in desc)):
                            draw_desc_text[app_id] = [id_generator(), patent_id, heading, draw_seq]
                        else:
                            pass  # skipping the brief description heading
            except Exception:
                pass
                #print "Problem with drawing description"

            try:
                brf_sum = avail_fields['BRFSUM'].split("\n")
                heading = "NULL"
                text = "NULL"
                brf_sum_seq = 0
                brf_text = ''
                for line in brf_sum:
                    if line.startswith("<p id"):
                        brf_sum_seq += 1
                        # this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        brf_sum, cut, end = text.partition("</p")
                        #brf_sum_text[id_generator()] = [patent_id,"text", brf_sum, brf_sum_seq]
                        brf_text += ' ' + brf_sum
                    if line.startswith("<heading"):
                        brf_sum_seq += 1
                        heading = re.search(">(.*?)<", line).group(1)
                        brf_text += " " + heading
                        #brf_sum_text[id_generator()] = [patent_id,"heading", heading, brf_sum_seq]
                brf_sum_text[app_id] = [id_generator(), patent_id, brf_text]
            except Exception:
                pass

            try:
                det = avail_fields["DETDESC"].split("\n")
                det_seq = 0
                heading = "NULL"
                detailed_text_field = ""
                for line in det:
                    if line.startswith("<p id"):
                        det_seq += 1
                        # this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        det_desc, cut, end = text.partition("</p")
                        # there are empty paragraph tags that flag lists and tables, for now we are skipping them
                        # maybe later import table or list from table and list parsing to improve completeness
                        # only include if there is a detailed desciption text in this tag
                        if len(det_desc) > 5:
                            detailed_text_field += " " + det_desc
                    if line.startswith("<heading"):
                        det_seq += 1
                        heading = re.search(">(.*?)<", line).group(1)
                        detailed_text_field += " " + heading
                        #detail_desc_text[id_generator()] = [patent_id,"heading", heading, det_seq]
                if ("<" in detailed_text_field) or (">" in detailed_text_field):
                    soup = bs(detailed_text_field, "lxml")
                    text = soup.get_text()
                else:
                    text = detailed_text_field
                #text = [piece.encode('utf-8','ignore') for piece in text]
                #text = "".join(text)
                detail_desc_text[app_id] = [id_generator(), patent_id, text, len(text)]
            except Exception:
                pass

            if 'us-term-of-grant' in avail_fields:
                us_term_of_grant_temp = avail_fields['us-term-of-grant']
                us_term_of_grant_temp = us_term_of_grant_temp.split('\n')
                lapse_of_patent = 'NULL'
                text = 'NULL'
                length_of_grant = 'NULL'
                us_term_extension = 'NULL'
                for line in us_term_of_grant_temp:
                    if line.startswith('<lapse-of-patent'):
                        lapse_of_patent = re.search('<lapse-of-patent>(.*?)</lapse-of-patent>', line).group(1)
                    if line.startswith('<text'):
                        text = re.search('<text>(.*?)</text>', line).group(1)
                    if line.startswith('<length-of-grant'):
                        length_of_grant = re.search('<length-of-grant>(.*?)</length-of-grant>', line).group(1)
                    if line.startswith('<us-term-extension'):
                        us_term_extension = re.search('<us-term-extension>(.*?)</us-term-extension>', line).group(1)
                us_term_of_grant[app_id] = [id_generator(), patent_id, lapse_of_patent, "NULL",
                                            text, length_of_grant, us_term_extension]

            if 'pct-or-regional-publishing-data' in avail_fields:
                number = None
                date = None
                country = None
                rel_id = None
                kind = None
                date_102 = None
                publishing = avail_fields["pct-or-regional-publishing-data"].split("\n")
                for line in publishing:
                    try:
                        if line.startswith("<doc-number"):
                            rel_id = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                    except Exception:
                        print("Publishing data lacks docno")
                    if line.startswith('<kind'):
                        kind = re.search('<kind>(.*?)</kind>', line).group(1)
                    if line.startswith('<date'):
                        date = re.search('<date>(.*?)</date>', line).group(1)
                        if date[6:] != "00":
                            date = date[:4]+'-'+date[4:6]+'-'+date[6:]
                        else:
                            date = date[:4]+'-' + date[4:6]+'-'+'01'
                    if line.startswith('<country'):
                        country = re.search('<country>(.*?)</country>', line).group(1)
                pct_data[app_id] = [id_generator(), patent_id, rel_id, date, None, country, kind, "wo_grant", None]

            if "pct-or-regional-filing-data" in avail_fields:
                rel_id = None
                number = None
                date = None
                country = None
                kind = None
                date_371 = None
                pct = avail_fields["pct-or-regional-filing-data"].split("<us-371c124-date")
                # split on us_371 date, because there are two date lines
                try:
                    pct_info = pct[0].split("\n")
                    info_371 = pct[1].split("\n")
                except Exception:
                    pct_info = pct[0].split("\n")
                    info_371 = []
                for line in pct_info:
                    if line.startswith("<doc-number"):
                        rel_id = re.search(
                            "<doc-number>(.*?)</doc-number>", line).group(1)
                    if line.startswith('<kind'):
                        kind = re.search('<kind>(.*?)</kind>', line).group(1)
                    if line.startswith('<date'):
                        date = re.search('<date>(.*?)</date>', line).group(1)
                        if date[6:] != "00":
                            date = date[:4]+'-'+date[4:6]+'-'+date[6:]
                        else:
                            date = date[:4]+'-' + date[4:6]+'-'+'01'
                    if line.startswith('<country'):
                        country = re.search('<country>(.*?)</country>', line).group(1)
                for line in info_371:
                    if line.startswith('<date'):
                        date3 = re.search('<date>(.*?)</date>', line).group(1)
                        if date3[6:] != "00":
                            date3 = date3[:4]+'-'+date3[4:6]+'-'+date3[6:]
                        else:
                            date3 = date3[:4]+'-' + date3[4:6]+'-'+'01'
                        date_371 = date3
                pct_data[app_id] = [id_generator(), patent_id, rel_id, date, date_371,
                                    country, kind, "pct_application", None]

            # us-issued (add to patents?)
            if "figures" in avail_fields:
                figures = avail_fields['figures'].split("\n")
                sheets = None
                figs = None
                for line in figures:
                    try:
                        if line.startswith('<number-of-drawing-sheets'):
                            sheets = re.search('<number-of-drawing-sheets>(.*?)</number-of-drawing-sheets>', line).group(1)
                        if line.startswith('<number-of-figures'):
                            figs = re.search('<number-of-figures>(.*?)</number-of-figures>', line).group(1)
                    except Exception:
                        print("Missing field from figures")
                figure_data[app_id] = [id_generator(), patent_id, figs, sheets]
            if "us-botanic" in avail_fields:
                latin = None
                variety = None
                botanic = avail_fields["us-botanic"].split("\n")
                for line in botanic:
                    try:
                        if line.startswith("<latin-name"):
                            latin_name = re.search('<latin-name>(.*?)</latin-name>', line).group(1)
                        if line.startswith("<variety"):
                            variety = re.search("<variety>(.*?)</variety>", line).group(1)
                    except Exception:
                        print("Problem with botanic")
                botanic_data[app_id] = [id_generator(), patent_id, appnum, latin_name, variety]

            patentdata[app_id] = [patent_id, apptype, docno, 'US', issdate, abst, title, patkind, numclaims, d]

            patfile = csv.writer(
                open(os.path.join(csv_dir, 'patent.csv'), 'ab'), delimiter='\t')
            for k, v in patentdata.items():
                patfile.writerow([k]+v)

            appfile = csv.writer(
                open(os.path.join(csv_dir, 'application.csv'), 'ab'), delimiter='\t')
            for k, v in application.items():
                appfile.writerow([k]+v)

            claimsfile = csv.writer(
                open(os.path.join(csv_dir, 'claim.csv'), 'ab'), delimiter='\t')
            for k, v in claims.items():
                claimsfile.writerow([k]+v)

            rawinvfile = csv.writer(
                open(os.path.join(csv_dir, 'rawinventor.csv'), 'ab'), delimiter='\t')
            for k, v in rawinventor.items():
                rawinvfile.writerow([k] + v)

            rawassgfile = csv.writer(
                open(os.path.join(csv_dir, 'rawassignee.csv'), 'ab'), delimiter='\t')
            for k, v in rawassignee.items():
                rawassgfile.writerow([k]+v)

            ipcrfile = csv.writer(
                open(os.path.join(csv_dir, 'ipcr.csv'), 'ab'), delimiter='\t')
            for k, v in ipcr.items():
                ipcrfile.writerow([k]+v)

            uspcfile = csv.writer(
                open(os.path.join(csv_dir, 'uspc.csv'), 'ab'), delimiter='\t')
            for k, v in uspc.items():
                uspcfile.writerow([k]+v)

            uspatentcitfile = csv.writer(
                open(os.path.join(csv_dir, 'uspatentcitation.csv'), 'ab'), delimiter='\t')
            for k, v in uspatentcitation.items():
                uspatentcitfile.writerow([k]+v)

            usappcitfile = csv.writer(
                open(os.path.join(csv_dir, 'usapplicationcitation.csv'), 'ab'), delimiter='\t')
            for k, v in usappcitation.items():
                usappcitfile.writerow([k]+v)

            foreigncitfile = csv.writer(
                open(os.path.join(csv_dir, 'foreigncitation.csv'), 'ab'), delimiter='\t')
            for k, v in foreigncitation.items():
                foreigncitfile.writerow([k]+v)

            otherreffile = csv.writer(
                open(os.path.join(csv_dir, 'otherreference.csv'), 'ab'), delimiter='\t')
            for k, v in otherreference.items():
                otherreffile.writerow([k]+v)

            rawlawyerfile = csv.writer(
                open(os.path.join(csv_dir, 'rawlawyer.csv'), 'ab'), delimiter='\t')
            for k, v in rawlawyer.items():
                rawlawyerfile.writerow([k]+v)

            examinerfile = csv.writer(
                open(os.path.join(csv_dir, 'rawexaminer.csv'), 'ab'), delimiter='\t')
            for k, v in examiner.items():
                examinerfile.writerow([k]+v)

            for_priorityfile = csv.writer(
                open(os.path.join(csv_dir, 'foreign_priority.csv'), 'ab'), delimiter='\t')
            for k, v in for_priority.items():
                for_priorityfile.writerow([k]+v)

            usreldocfile = csv.writer(
                open(os.path.join(csv_dir, 'usreldoc.csv'), 'ab'), delimiter='\t')
            for k, v in usreldoc.items():
                usreldocfile.writerow([k]+v)

            us_term_of_grantfile = csv.writer(
                open(os.path.join(csv_dir, 'us_term_of_grant.csv'), 'ab'), delimiter='\t')
            for k, v in us_term_of_grant.items():
                us_term_of_grantfile.writerow([k]+v)

            non_inventor_applicantfile = csv.writer(
                open(os.path.join(csv_dir, 'non_inventor_applicant.csv'), 'ab'), delimiter='\t')
            for k, v in non_inventor_applicant.items():
                non_inventor_applicantfile.writerow([k]+v)

            draw_desc_textfile = csv.writer(
                open(os.path.join(csv_dir, 'draw_desc_text.csv'), 'ab'), delimiter='\t')
            for k, v in draw_desc_text.items():
                try:
                    draw_desc_textfile.writerow([k]+v)
                except Exception:
                    first = v[0]
                    second = v[1]
                    third = v[2]
                    second = [piece.encode('utf-8', 'ignore') for piece in second]
                    second = "".join(second)
                    value = []
                    value.append(first)
                    value.append(second)
                    value.append(third)
                    draw_desc_textfile.writerow([k]+value)

            brf_sum_textfile = csv.writer(
                open(os.path.join(csv_dir, 'brf_sum_text.csv'), 'ab'), delimiter='\t')
            for k, v in brf_sum_text.items():
                brf_sum_textfile.writerow([k]+v)

            detail_desc_textfile = csv.writer(
                open(os.path.join(csv_dir, 'detail_desc_text.csv'), 'ab'), delimiter='\t')
            for k, v in detail_desc_text.items():
                try:
                    detail_desc_textfile.writerow([k]+v)
                except Exception:
                    dd_pat_id = v[0]
                    dd_len = v[2]
                    dd_text = v[1]
                    dd_text = [piece.encode('utf-8', 'ignore') for piece in dd_text]
                    dd_text = "".join(dd_text)
                    value = []
                    value.append(dd_pat_id)
                    value.append(dd_text)
                    value.append(dd_len)
                    detail_desc_textfile.writerow([k]+value)

            rel_app_textfile = csv.writer(
                open(os.path.join(csv_dir, 'rel_app_text.csv'), 'ab'), delimiter='\t')
            for k, v in rel_app_text.items():
                rel_app_textfile.writerow([k]+v)

            pct_datafile = csv.writer(
                open(os.path.join(csv_dir, 'pct_data.csv'), 'ab'), delimiter='\t')
            for k, v in pct_data.items():
                pct_datafile.writerow([k]+v)

            botanicfile = csv.writer(
                open(os.path.join(csv_dir, 'botanic.csv'), 'ab'), delimiter='\t')
            for k, v in botanic_data.items():
                botanicfile.writerow([k]+v)

            figuresfile = csv.writer(
                open(os.path.join(csv_dir, 'figures.csv'), 'ab'), delimiter='\t')
            for k, v in figure_data.items():
                figuresfile.writerow([k]+v)

    rawlocfile = csv.writer(
        open(os.path.join(csv_dir, 'rawlocation.csv'), 'ab'), delimiter='\t')
    for k, v in rawlocation.items():
        rawlocfile.writerow([k]+v)

    mainclassfile = csv.writer(
        open(os.path.join(csv_dir, 'mainclass.csv'), 'ab'), delimiter='\t')
    for k, v in mainclassdata.items():
        mainclassfile.writerow(v)

    subclassfile = csv.writer(
        open(os.path.join(csv_dir, 'subclass.csv'), 'ab'), delimiter='\t')
    for k, v in subclassdata.items():
        subclassfile.writerow(v)

    print(numi)
