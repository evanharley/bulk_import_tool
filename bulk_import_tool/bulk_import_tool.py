import pyodbc
import openpyxl

# Tools for the Bulk Import of Natural History Specimens
class import_tools():

    def __init__(self, *args, **kwargs):
        
        # Discipline should be gotten from user at the start of the import
        # so when coding GUI it should be included
        self.discipline = '' 
        self._connection = pyodbc.connect('DSN=ImportTest; Trusted_Connection=yes;')
        self.cursor = self._connection.cursor()
    
    def _get_file(self, filename):
       
        try:
            self.data_file = openpyxl.load_workbook(filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file['IMM_template']

    def _find_persons(self):
        # Return all unique persons in the spreadsheet for import
        # Persons to be a dict in format {personName: [ids]}
        person_cols = self._find_relevant_column('Person')
        names = []
        persons = {}
        for row in range (4, self.ws.max_row + 1):
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
                name = names[i].split(',')
                name = [thing.strip() for thing in names]
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
            table_id = ['CollectionEvent.bait',
                        'CollectionEvent.collec_method',
                        'CollectionEvent.collection_date',	
                        'CollectionEvent.date_remarks',
                        'CollectionEvent.discipline_cd',
                        'CollectionEvent.event_num',
                        'CollectionEvent.field_event',	
                        'CollectionEvent.net_gear_trap',
                        'CollectionEvent.note',	
                        'CollectionEvent.permit_num', 
                        'CollectionEvent.season',	
                        'CollectionEvent.start_time',
                        'CollectionEvent.stop_time', 
                        'CollectionEvent.time_standard', 
                        'CollectionEvent.sampling_duration',
                        'CollectionEvent.vessel_name',	
                        'CollectionEvent.air_temp', 
                        'CollectionEvent.at_unit', 
                        'CollectionEvent.cloud_cover',	
                        'CollectionEvent.weather_remarks',	
                        'CollectionEvent.wind_direction',
                        'CollectionEvent.wind_speed',
                        'CollectionEvent.ws_unit']
        elif method == 'Sites':
            table_id = ['GeographicSite.max_easl',
                             'GeographicSite.min_easl',
                             'GeographicSite.note_easl',
                             'GeographicSite.unit_easl',
                             'GeographicSite.biogeoclimatic',	
                             'GeographicSite.biozone',
                             'GeographicSite.continent',
                             'GeographicSite.country',
                             'GeographicSite.county',
                             'GeographicSite.district',
                             'GeographicSite.ecoprovince',
                             'GeographicSite.fossile_ref_num',
                             'GeographicSite.mine_name',
                             'GeographicSite.natural_region',
                             'GeographicSite.park',
                             'GeographicSite.prov_state',
                             'GeographicSite.township',
                             'GeographicSite.water_body',
                             'GeographicSite.collector_site_id',
                             'GeographicSite.description',
                             'GeographicSite.discipline_cd',
                             'GeographicSite.location_name',
                             'GeographicSite.reference',
                             'GeographicSite.remarks',
                             'GeoSiteNote.note_date',
                             'GeoSiteNote.note',
                             'GeoSiteNote.title',
                             'GeographicSite.accuracy',
                             'GeographicSite.latlong_approximate',
                             'GeographicSite.latitude',
                             'GeographicSite.latitude_stop',
                             'GeographicSite.longitude',
                             'GeographicSite.longitude_stop',
                             'GeographicSite.na_datapoint',
                             'GeographicSite.non_nts_map_ref',
                             'GeographicSite.nts_ref',
                             'GeographicSite.utm_datapoint',
                             'GeographicSite.utm_east',
                             'GeographicSite.utm_north',
                             'GeographicSite.utm_zone',
                             'GeographicSite.primary_river_drainage',
                             'GeographicSite.secondary_river_drainage',	
                             'GeographicSite.tertiary_river_drainage']

        for col in range(1, len(headder_row)):
            if headder_row[col].value in table_id and col not in relevant_cols:
                relevant_cols.append(col)
        return relevant_cols
        
    def _split_persons(self, person_names):
        # Returns the split value of person names where a dilineator is present
        names = []
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
        for row in range (4, self.ws.max_row + 1):
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
                self.ws.cell(row = row, column = 51, value = site_id)
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
                    self.ws.cell(row = row, column = 51, value = site_id)
                    generated_sites[site_id]["Collector's Site ID"] = site_id
                else:
                    self.ws.cell(row = row, column = 51, value = matching_id)
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
        prefix_query = "Select coll_event_prefix from NHDisciplineType where discipline_cd = '{}'".format(self.discipline)
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
                self.ws.cell(row = row, column = 14, value = event_id)
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
                    self.ws.cell(row = row, column = 14, value = event_id)
                    generated_events[event_id]["Event Number"] = event_id
                else:
                    self.ws.cell(row = row, column = 14, value = matching_id)
                
        return generated_events

    def _write_persontaxa(self, data, section):
        row = 1
        col = 'A'
        work_sheet = self.data_file[section]
        sheet_ref = sheet_ref = chr(ord(col)) + str(row)
        work_sheet[sheet_ref] = section
        sheet_ref = sheet_ref = chr(ord(col) + 1) + str(row)
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
            worksheet.cell(row = row, column = col, value = 'Event Number') 
        else:
            worksheet.cell(row = row, column = col, value =  "Collector's Site ID")
        first_record = data[list(data.keys())[1]]
        keys = [key for key in first_record.keys()]
        for key in data.keys():
            if row == 1:
                worksheet.cell(row = row + 1, column = 1, value = key)
            else:
                worksheet.cell(row = row, column = 1, value = key)
            if row == 1:
                for i in range(len(keys)):
                    worksheet.cell(row = row, column = col + 1 + i, value = keys[i])
                row += 1

            for i in range(len(keys)):
                if data[key][keys[i]] is None:
                    continue
                else:
                    if isinstance(data[key][keys[i]], list):
                        names = '; '.join(data[key][keys[i]])
                        worksheet.cell(row = row, column = col + 1 + i, value = names)
                    else:
                        worksheet.cell(row = row, column = col + 1 + i, value = data[key][keys[i]])
            row += 1
        return 0

    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        missing = set(['IMM_template', 'Person', 'Taxon', 'Site', 'Event']) - set(self.data_file.sheetnames)
        if len(missing) > 0:
            for sheet in missing:
                self.data_file.create_sheet(sheet)

        persons = self._find_persons()
        self._write_persontaxa(persons, 'Person')
        print("Persons Complete")

        taxa = self._find_taxa()
        self._write_persontaxa(taxa, 'Taxon')
        print("Taxa Complete")

        sites = self._generate_sites()
        self._write_siteevent(sites, "Site")
        print("Sites Complete")

        events = self._generate_events()
        self._write_siteevent(events, 'Event')
        print("Events Complete")

        self.data_file.save(self.data_filename[:-5] + '_test.xlsx')
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
            query_field = '[' + value[value.find('.') + 1 :] + ']'
            if query_table == '[[DISCIPLINE]]':
                query_table = '[' + disciplines[self.discipline] + 'Item' + ']'
            query = "select {} from {}".format(query_field, query_table)
            try:
                test = self.cursor.execute(query).fetchone()
                test_results[value] = True
            except:
                test_results[value] = False
        return test_results
    
    def _check_sheets(self):
        if set(self.data_file.sheetnames) == set(['IMM_template', 'Person', 'Taxon', 'Site', 'Event']):
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
                data[sheet] = {workingsheet.cell(i, 1).value: workingsheet.cell(i, 2).value for i in range (2, workingsheet.max_row + 1)}
            else:
                keys = [workingsheet.cell(row = 1, column=i).value for i in range(1, workingsheet.max_column + 1)] 
                for row in range(2, workingsheet.max_row + 1):
                    id = workingsheet.cell(row = row, column = 1).value
                    data[sheet][id] = {keys[i - 1]: workingsheet.cell(row = row, column = i).value for i in range(1, workingsheet.max_column + 1)}
            if sheet in ['Person', 'Taxon']:
                self._handle_persontaxa(data)
            if sheet in ['Site', 'Event']:
                self._handle_siteevent(data)
            
        self.data_file.save(self.data_filename)
        return 0, 'Done'


    def _handle_persontaxa(self, data):
        tab = list(data.keys())[0]
        data = data[tab]
        relevant_cols = self._find_relevant_column(tab)
        i = 1
        for col in relevant_cols:
            col = col + i
            self.ws.insert_cols(col)
            self.ws.cell(row = 3, column = col, value = tab + '_id')

            for row in range(4,self.ws.max_row + 1):
                value = self.ws.cell(row = row, column = (col + 1)).value
                if value is None:
                    continue

                if tab == 'Person':
                    values = self._split_persons(value)
                    if isinstance(values, list):
                        value = '; '.join([str(data[thing]) for thing in values])
                    else:
                        value = str(data[value])
                    self.ws.cell(row = row, column = col, value = value)
                else:
                    self.ws.cell(row = row, column = col, value = data[value])
            i += 1
        return 0

    def _handle_siteevent(self, data):
        tab = list(data.keys())[0]
        data = data[tab]
        tab = tab + 's'
        relevant_cols = self._find_relevant_column(tab)
        for col in relevant_cols:
            key = self.ws.cell(row = 2, column = col + 1).value

            for row in range(4, self.ws.max_row + 1):
                value = self.ws.cell(row = row, column = col + 1).value
                if tab == 'Sites':
                    id = self.ws.cell(row = row, column = 52).value
                else:
                    id = self.ws.cell(row = row, column = 14).value
                if value != data[id][key]:
                    self.ws.cell(row = row, column = col + 1, value = data[id][key])
        return 0

    def _to_prod(self):
        self._connection = pyodbc.connect('DSN=IMM Prod; Trusted_Connection=yes;')
        return 0, "Database connection changed to Production"

    def _to_test(self):
        self._connection = pyodbc.connect('DSN=ImportTest; Trusted_Connection=yes;')
        return 0, 'Database connection changed to Test'

    def _import_site(self):
        return 0

    def _import_event(self):
        return 0

    def _import_persons(self):
        return 0

    def _import_specimen(self):
        return 0

    def write_to_db():
        # Writes the data from the import spreadsheet to the database
        return 0

