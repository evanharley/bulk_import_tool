import pyodbc
import openpyxl
from pubsub import pub

# Tools for the Bulk Import of Natural History Specimens


class ImportTools:

    def __init__(self, *args, **kwargs):
        
        # Discipline should be gotten from user at the start of the import
        # so when coding GUI it should be included
        self.data_filename = ''
        self.discipline = '' 
        self._connection = pyodbc.connect('DSN=ImportTest; Trusted_Connection=yes;')
        self.cursor = self._connection.cursor()
        self.data_file = None
        self.ws = None
        self.write_status = {'GeographicSite': False, 
                             'CollectionEvent': False,
                             'Taxonomy': False,
                             'Triggers': False}
        self.proc_log = []
        
    
    def _get_file(self, filename):
        self.data_filename = filename
        try:
            self.data_file = openpyxl.load_workbook(filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file['IMM_template']

    def _write_prog(self):
        prog_log = open('prog_log.log', 'a')
        prog_log.write('{0}: {1}'.format(self.data_filename, ', '.join(self.proc_log)))
        return 0

    def _get_prog_info(self):
        temp = ''
        try:
            prog_log = open('prog_log.log', 'r')
        except FileNotFoundError:
            self.proc_log = []
            return 0
        for row in prog_log:
            if row.startswith(self.data_filename):
                temp = row[row.find(': ') + 1:].strip(' ').split(', ')
        self.proc_log = temp
        return 0

    def _find_persons(self):
        # Return all unique persons in the spreadsheet for import
        # Persons to be a dict in format {personName: [ids]}
        person_cols = self._find_relevant_column('Person')
        names = []
        persons = {}
        for row in range(4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in person_cols:
                if row_data[column].value is not None:
                    name = self._split_persons(row_data[column].value)
                    if isinstance(name, list):
                        names.extend(name)
                    else:
                        names.append(name)
                else:
                    continue
        for i in range(len(names)):
            if ',' in names[i]:
                name = [thing.strip() for thing in names[i].split(',')]
                names[i] = ' '.join(name)
        names = list(set(names))

        for name in names:
            query = "Select person_id from Person where search_name = '{}'".format(name)
            results = self.cursor.execute(query).fetchall()
            persons[name] = []
            if results != []:
                for i in range(len(results)):
                    persons[name].append(results[i][0])
            else:
                persons[name] = ['NEW?']
        return persons

    def _get_full_disc(self):
        disciplines = {
            'bot': 'Botany',
            'ent': 'Entomology',
            'geo': 'Geology',
            'her': 'Herpetology',
            'ich': 'Ichthyology',
            'inv': 'Invertebrate',
            'mam': 'Mammalogy',
            'orn': 'Ornithology',
            'pal': 'Paleontology'
            }
        return disciplines[self.discipline]

    def _find_relevant_column(self, method):
        # Return the index of the columns relevant to the _find methods above.
        # Values in a list of indices
        headder_row = self.ws[3]
        relevant_cols = []
        table_id = ''
        if method == 'Person':
            table_id = ['Person.person_id']
        elif method == 'Taxon':
            table_id = ['Taxonomy.taxon_id']
        elif method == 'Events':
            table_id = ['CollectionEvent.']
        elif method == 'Sites':
            table_id = ['GeographicSite.', 'GeoSiteNote']
        elif method == 'Item':
            table_id = ['Item']
        elif method == 'NHItem':
            table_id = ['NaturalHistoryItem.']
        elif method == 'FieldMeasurement':
            table_id = ['FieldMeasurement.']
        elif method == 'DisciplineItem':
            disc = self.get_full_disc()
            table_id = ['[DISCIPLINE].', disc + 'Item.']
        elif method == 'ImptPerson':
            table_id = ['Person_id']
        elif method == 'ImptTaxon':
            table_id = ['Taxonomy']
        elif method == 'Preparation':
            table_id = ['Preparation.']
        elif method == 'ChemicalTreatment':
            table_id = ['ChemicalTreatment']

        for col in range(1, len(headder_row)):
            if any(headder_row[col].value.startswith(id) for id in table_id) \
            and col not in relevant_cols:
                relevant_cols.append(col)
        return relevant_cols
        
    def _split_persons(self, person_names):
        # Returns the split value of person names where a delineator is present
        delineators = ";:|/\\"
        if any(char in person_names for char in delineators):
            person_names = person_names.replace(';', ',').replace(':', ',').replace('|', ',')
            names = [name.strip(' ') for name in person_names.split(',')]
        else:
            names = person_names
        return names

    def _find_taxa(self):
        # Returns the unique taxa for each taxon in the import spreadsheet
        # Taxa to be a dict in format {scientificName: [ids]}
        taxon_cols = self._find_relevant_column('Taxon')
        taxa = {} 
        sns = []
        for row in range(4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in taxon_cols:
                if row_data[column].value is not None:
                    sns.append(row_data[column].value)
                else:
                    continue
        sns = list(set(sns))

        for sn in sns:
            taxa[sn] = []
            taxa[sn].extend(self._query_taxa(sn))

        return taxa

    def _query_taxa(self, sn):
        # Returns the taxon ids of a scientific name
        if not sn.endswith('sp.'):
            query = "Select * from ScientificName where scientific_name ='{}'".format(sn)
        else:
            sn = sn[: sn.find(' ')]
            query = "Select taxon_id, term from taxon where term = '{}'".format(sn)
        results = self.cursor.execute(query).fetchall()
        if results != []:
            taxa = [result[0] for result in results]
        else:
            taxa = ["NEW?"]
            
        return taxa

    def _generate_sites(self):
        # Generates new sites for import, from unique sites in the import spreasheet
        new_site_id = self._get_max_site_id()
        relevant_cols = self._find_relevant_column('Sites')
        key_row = self.ws[2]
        generated_sites = {}
        for row in range(4, self.ws.max_row + 1):
            new_site_id[1] = str(int(new_site_id[1]) + 1)
            site_id = new_site_id[0] + new_site_id[1]
            if generated_sites == {}:
                generated_sites[site_id] = {}
                for index in relevant_cols:
                    generated_sites[site_id][key_row[index].value] = self.ws[row][index].value
                generated_sites[site_id]["Collector's Site ID"] = site_id
                self.ws.cell(row=row, column=51, value=site_id)
            else:
                site = {}
                difference = 0
                for index in relevant_cols:
                    site[key_row[index].value] = self.ws[row][index].value
                for item in generated_sites.keys():
                    diff = {key: site[key] for key in generated_sites[item] 
                            if key != "Collector's Site ID" and site[key] != generated_sites[item][key]}
                    if len(diff.keys()) > 0:
                        difference += 1
                    else:
                        difference = 0
                        matching_id = item
                        break
                if difference > 0:
                    generated_sites[site_id] = site
                    self.ws.cell(row=row, column=51, value=site_id)
                    generated_sites[site_id]["Collector's Site ID"] = site_id
                else:
                    self.ws.cell(row=row, column=51, value=matching_id)
        return generated_sites

    def _get_max_site_id(self):
        prefix_query = "Select geo_site_prefix from NHDisciplineType where discipline_cd = '{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(collector_site_id, 3, 100))) from GeographicSite " + \
            "where discipline_cd = '{}' and substring(collector_site_id, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        max_site_id = [prefix, str(result[0])]
        return max_site_id

    def _get_max_event_id(self):
        prefix_query = "Select coll_event_prefix from NHDisciplineType where discipline_cd = " +\
                                "'{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(event_num, 3, 100))) from CollectionEvent " + \
            "where discipline_cd = '{}' and substring(event_num, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        max_event_id = [prefix, str(result[0])]
        return max_event_id

    def _generate_events(self):
        # Generates new collection events for import, from the unique events in the import spreadsheet
        generated_events = {}
        new_event_id = self._get_max_event_id()
        relevant_cols = self._find_relevant_column('Events')
        key_row = self.ws[2]
        for row in range(4, self.ws.max_row + 1):
            new_event_id[1] = str(int(new_event_id[1]) + 1)
            event_id = new_event_id[0] + new_event_id[1]
            working_row = self.ws[row]
            if generated_events == {}:
                generated_events[event_id] = {}
                for index in relevant_cols:
                    generated_events[event_id][key_row[index].value] = working_row[index].value
                generated_events[event_id]["Event Number"] = event_id
                self.ws.cell(row=row, column=14, value=event_id)
            else:
                event = {}
                difference = 0
                for index in relevant_cols:
                    event[key_row[index].value] = working_row[index].value
                for item in generated_events.keys():
                    diff = {key: event[key] for key in generated_events[item] 
                            if key != "Event Number" and event[key] != generated_events[item][key]}
                    if len(diff.keys()) > 0:
                        difference += 1
                    else:
                        difference = 0
                        matching_id = item
                        break
                if difference > 0:
                    generated_events[event_id] = event
                    self.ws.cell(row=row, column=14, value=event_id)
                    generated_events[event_id]["Event Number"] = event_id
                else:
                    self.ws.cell(row=row, column=14, value=matching_id)
                
        return generated_events

    def _write_persontaxa(self, data, section):
        row = 1
        col = 'A'
        work_sheet = self.data_file[section]
        sheet_ref = chr(ord(col)) + str(row)
        work_sheet[sheet_ref] = section
        sheet_ref = chr(ord(col) + 1) + str(row)
        work_sheet[sheet_ref] = section + '_ids'
        row += 1
        for key in data.keys():
            sheet_ref = col + str(row)
            work_sheet[sheet_ref] = key
            for i in range(len(data[key])):
                sheet_ref = chr(ord(col) + 1 + i) + str(row)
                work_sheet[sheet_ref] = data[key][i]
            row += 1
        return 0

    def _write_siteevent(self, data, section):
        row = 1
        col = 1
        worksheet = self.data_file[section]
        if section == 'Event':
            worksheet.cell(row=row, column=col, value='Event Number')
        else:
            worksheet.cell(row=row, column=col, value="Collector's Site ID")
        first_record = data[list(data.keys())[1]]
        keys = [key for key in first_record.keys()]
        for key in data.keys():
            if row == 1:
                worksheet.cell(row=row + 1, column=1, value=key)
            else:
                worksheet.cell(row=row, column=1, value=key)
            if row == 1:
                for i in range(len(keys)):
                    worksheet.cell(row=row, column=col + 1 + i, value=keys[i])
                row += 1

            for i in range(len(keys)):
                if data[key][keys[i]] is None:
                    continue
                else:
                    if isinstance(data[key][keys[i]], list):
                        names = '; '.join(data[key][keys[i]])
                        worksheet.cell(row=row, column=col + 1 + i, value=names)
                    else:
                        worksheet.cell(row=row, column=col + 1 + i, value=data[key][keys[i]])
            row += 1
        return 0

    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        missing = {'IMM_template', 'Person', 'Taxon', 'Site', 'Event'} - set(self.data_file.sheetnames)
        if len(missing) > 0:
            for sheet in missing:
                self.data_file.create_sheet(sheet)

        persons = self._find_persons()
        self._write_persontaxa(persons, 'Person')
        pub.sendMessage('UpdateMessage', arg1="Persons Complete")

        taxa = self._find_taxa()
        self._write_persontaxa(taxa, 'Taxon')
        pub.sendMessage('UpdateMessage', arg1="Taxa Complete")

        sites = self._generate_sites()
        self._write_siteevent(sites, "Site")
        pub.sendMessage('UpdateMessage', arg1="Sites Complete")

        events = self._generate_events()
        self._write_siteevent(events, 'Event')
        pub.sendMessage('UpdateMessage', arg1="Events Complete")

        self.data_file.save(self.data_filename[:-5] + '_test.xlsx')
        self.proc_log.append('Write Spreadsheet')
        return 0 

    def _test_spreadsheet(self):
        key_row = self.ws[3]
        test_results = {}
        disciplines = {
            'bot': 'Botany',
            'ent': 'Entomology',
            'geo': 'Geology',
            'her': 'Herpetology',
            'ich': 'Ichthyology',
            'inv': 'Invertebrate',
            'mam': 'Mammalogy',
            'orn': 'Ornithology',
            'pal': 'Paleontology'
            }
        for key in key_row:
            value = key.value
            query_table = '[' + value[:value.find('.')] + ']'
            query_field = '[' + value[value.find('.') + 1:] + ']'
            if query_table == '[[DISCIPLINE]]':
                query_table = '[' + disciplines[self.discipline] + 'Item' + ']'
            query = "select {} from {}".format(query_field, query_table)
            try:
                test_results = self.cursor.execute(query).fetchone()
                test_results[value] = True
            except:
                test_results[value] = False
        return test_results
    
    def _check_sheets(self):
        if set(self.data_file.sheetnames) == ('IMM_template', 'Person', 'Taxon', 'Site', 'Event'):
            return True
        else:
            return False

    def _check_persontaxa(self):
        for sheet in ['Person', 'Taxon']:
            workingsheet = self.data_file[sheet]
            if workingsheet.max_column > 2:
                return 1, 'This {} sheet is incomplete'.format(sheet)
            for row in range(2, workingsheet.max_row + 1):
                if not str(workingsheet.cell(row, 2).value).isnumeric():
                    return 1, 'This {} sheet is incomplete'.format(sheet)
        return 0, 'Complete'

    def _add_ids(self):
        if not self._check_sheets:
            return 1, 'This is the wrong spreadsheet'
        if not self._check_persontaxa:
            return 1, 'Persons/Taxa has not been completed'
        
        for sheet in ['Person', 'Taxon', 'Site', 'Event']:
            data = {sheet: {}}
            workingsheet = self.data_file[sheet]
            if sheet in ['Person', 'Taxon']:
                data[sheet] = {workingsheet.cell(i, 1).value: workingsheet.cell(i, 2).value
                               for i in range(2, workingsheet.max_row + 1)}
            else:
                keys = [workingsheet.cell(row=1, column=i).value for i in range(1, workingsheet.max_column + 1)]
                for row in range(2, workingsheet.max_row + 1):
                    id = workingsheet.cell(row=row, column=1).value
                    data[sheet][id] = {keys[i - 1]: workingsheet.cell(row=row, column=i).value
                                       for i in range(1, workingsheet.max_column + 1)}
            if sheet in ['Person', 'Taxon']:
                self._handle_persontaxa(data)
            if sheet in ['Site', 'Event']:
                self._handle_siteevent(data)
            pub.sendMessage('UpdateMessage', arg1="{} Complete".format(sheet))
            
        self.data_file.save(self.data_filename)
        self.proc_log.append('IDs added')
        return 0, 'Done'

    def _handle_persontaxa(self, data):
        tab = list(data.keys())[0]
        data = data[tab]
        relevant_cols = self._find_relevant_column(tab)
        i = 1
        for col in relevant_cols:
            col = col + i
            self.ws.insert_cols(col)
            self.ws.cell(row=3, column=col, value=tab + '_id')

            for row in range(4, self.ws.max_row + 1):
                value = self.ws.cell(row=row, column=(col + 1)).value
                if value is None:
                    continue

                if tab == 'Person':
                    values = self._split_persons(value)
                    if isinstance(values, list):
                        value = '; '.join([str(data[thing]) for thing in values])
                    else:
                        value = str(data[value])
                    self.ws.cell(row=row, column=col, value=value)
                else:
                    self.ws.cell(row=row, column=col, value=data[value])
            i += 1
        return 0

    def _handle_siteevent(self, data):
        tab = list(data.keys())[0]
        data = data[tab]
        tab = tab + 's'
        relevant_cols = self._find_relevant_column(tab)
        for col in relevant_cols:
            key = self.ws.cell(row=2, column=col + 1).value

            for row in range(4, self.ws.max_row + 1):
                value = self.ws.cell(row=row, column=col + 1).value
                if tab == 'Sites':
                    id = self.ws.cell(row=row, column=52).value
                else:
                    id = self.ws.cell(row=row, column=14).value
        return 0

    def _to_prod(self):
        self._connection = pyodbc.connect('DSN=IMM Prod; Trusted_Connection=yes;')
        return 0, "Database connection changed to Production"

    def _to_test(self):
        self._connection = pyodbc.connect('DSN=ImportTest; Trusted_Connection=yes;')
        return 0, 'Database connection changed to Test'

    def _import_site(self):
        pub.sendMessage('UpdateMessage', arg1="Writing Sites",
                        arg2 = 1,
                        arg3 = self.data_file['Site'].max_row)
        relevant_cols = self._find_relevant_column('Sites')
        keys = {self.ws[2][col].value: self.ws[3][col].value 
                for col in relevant_cols}
        max_query = "Select Max(geo_site_id) from GeographicSite"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Site']
        self._set_identity_insert('GeographicSite')
        for row in range(2, working_sheet.max_row):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column + 1) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'geo_site_id, discipline_cd, '
            insert_keys += ', '.join([keys[item].split('.')[1] for item in data.keys()])
            query_part_1 = "INSERT INTO GeographicSite({})".format(insert_keys)
            query_part_2 = "VALUES ({}, '{}'".format(max_id, self.discipline)
            for datum in data.keys():
                if isinstance(data[datum], str):
                    value = "'{}'".format(data[datum])
                else:
                    value = data[datum]
                query_part_2 += ", {}".format(value)
            query = query_part_1 + ' \n' + query_part_2 + ')'
            item = data["Collector's Site ID"]
            pub.sendMessage('UpdateMessage', arg1="{} written to db".format(item))
            self.cursor.execute(query)
        self._set_identity_insert('GeographicSite')
        return 0

    def _import_event(self):
        pub.sendMessage('UpdateMessage', arg1="Writing Events",
                        arg2 = 1,
                        arg3 = self.data_file['Event'].max_row)
        relevant_cols = self._find_relevant_column('Events')
        keys = {self.ws[2][col].value: self.ws[3][col].value 
                for col in relevant_cols}
        max_query = "Select Max(coll_event_id) from CollectionEvent"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Event']
        self._set_identity_insert('CollectionEvent')
        for row in range(2, working_sheet.max_row):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column + 1) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'coll_event_id, discipline_cd, '
            insert_keys += ', '.join([keys[item].split('.')[1] for item in data.keys()])
            query_part_1 = "INSERT INTO CollectionEvent({})".format(insert_keys)
            query_part_2 = "VALUES ({}, '{}'".format(max_id, self.discipline)
            for datum in data.keys():
                if isinstance(data[datum], str):
                    value = "'{}'".format(data[datum])
                else:
                    value = data[datum]
                query_part_2 += ", {}".format(value)
            query = query_part_1 + ' \n' + query_part_2 + ')'
            item = data["Event Number"]
            pub.sendMessage('UpdateMessage', arg1="{} written to db".format(item))
            self.cursor.execute(query)
        self._set_identity_insert('GeographicSite')
        return 0

    def _import_site_event(self):
        site_event = []
        for row in range(4, self.ws.max_row + 1):
            site = self.ws[row][52]
            event = self.ws[row][14]
            site_event.append(selif.query_site_event(site, event))
        query = '''Insert into GeographicSite_CollectionEvent(geo_site_id, coll_event_id)
                    Values ({0}, {1})'''
        site_event = set(site_event)
        for pair in site_event:
            self.cursor.execute(query.format(pair[0], pair[1]))

        return 0

    def _query_site_event(self, site_event: tuple):
        site = site_event[0]
        event = site_event[1]

        site_query = "Select geo_site_id from GeographicSite where collector_site_id = '{}'"
        event_query = "Select coll_event_id from CollectionEvent where event_num = '{}'"

        site = self.cursor.execute(site_query.format(site))
        event = self.cursor.execute(event_query.format(event))

        return (site, event)

    def _import_specimen(self):
        pub.sendMessage('UpdateMessage', arg1="Writing Specimens",
                        arg2 = 1,
                        arg3 = self.ws.max_row)
        for row in range(4, self.ws.max_row + 1):
            data_row = self.ws[row]
            max_query = "Select Max(item_id) from Item"
            max_id = self.cursor.execute(max_query).fetchone()[0] + 1 
            item = self._prep_item(data_row)
            nhitem = self._prep_nhitem(data_row)
            disc_item = self._prep_discipline_item(data_row)
            preparation = self._prep_preparation(data_row)
            taxonomy = self._prep_taxon(data_row)
            persons = self._prep_persons(data_row)

            for process in [item, nhitem, disc_item, preparation, taxonomy, persons]:
                print('stuff')
            pub.sendMessage('UpdateMessage', arg1='')
        return 0

    def _prep_item(self, row):
        relevant_cols = self._find_relevant_column('Item')
        item = {self.ws[3][col].value[6:]: row[col].value 
                for col in relevant_cols}        
        return item

    def _prep_nhitem(self, row):
        stuff = 'stuff'
        print(stuff)
        return stuff

    def _prep_discipline_item(self, row):
        stuff = 'stuff'
        print(stuff)
        return stuff

    def _prep_preparation(self, row):
        stuff = 'stuff'
        print(stuff)
        return stuff

    def _prep_taxon(self, row):
        print('stuff')

    def _prep_persons(self, row):
        print('stuff')

    def _set_identity_insert(self, table):
        if self.write_status[table] == False:
            query = 'set identity_insert {} on;'.format(table)
        else:
            query = 'set identity_insert {} off;'.format(table)
        self.cursor.execute(query)
        self.write_status[table] = not self.write_status[table]
        return 0

    def _set_triggers(self):
        if self.write_status['Triggers'] == False:
            status = 'DISABLE'
        else:
            status = 'ENABLE'
        query = '''{0} TRIGGER create_sname ON Taxon;
                   {0} Trigger set_is_component on Component;
                   {0} TRIGGER set_qualified_name ON Taxonomy;
                   {0} TRIGGER clear_item_name ON Taxonomy;
                   {0} TRIGGER update_person_search_name ON Person;
                   {0} TRIGGER update_artist_search_name ON Artist;
                   {0} TRIGGER create_sname on Taxon;
                   {0} TRIGGER create_location_code on Location;'''.format(status)
        self.cursor.execute(query)

    def write_siteevent_to_db(self):
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to off',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self._import_site()
        self._import_event()
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to on',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1 = 'Complete!!')
        self.proc_log.append('Import GeographicSite and CollectionEvent')

    def write_specimen_taxa_persons_to_db(self):
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to off',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self._import_specimen()
        self._import_taxa()
        self._import_persons()
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to on',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1 = 'Complete!!')
        self.proc_log.append('Import Complete')

    def write_to_db(self):
        # Writes the data from the import spreadsheet to the database
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to off',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self._import_site()
        self._import_event()
        self._import_specimen()
        self._import_taxa()
        self._import_persons()
        pub.sendMessage('UpdateMessage', arg1 = 'Setting Triggers to on',
                        arg2 = 1, arg3 = 1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1 = 'Complete!!')
        self.proc_log.append('Import Complete')
        return 0

