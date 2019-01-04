import unittest
from bulk_import_tool import import_tools

class BulkImportTest(unittest.TestCase):
    
    def setUp(self):
        self.impt = import_tools()
    
    def test_find_persons(self):
        value = self.impt._find_persons()
        test_values = {'Hugh MacIntosh': [6755], 'Evan Harley': [6168],
                       'Meg Sugrue': [], 'David Stewart': [],
                       'Henry Choong': [2767, 4659], 'Heidi Gartner': [2430, 4829, 5698]}
        self.assertEqual(test_values, value)

    def test_find_relevant_column(self):
        test_methods = ['Person', 'Taxon', 'Sites', 'Events']
        value = []
        test_values = [22, 11, 59, 61, 75, 76, 117]
        for method in test_methods:
            value.extend(self.impt._find_relevant_column(method))
        self.assertEqual(sorted(test_values), sorted(value))

    def test_split_persons(self):
        value = self.impt._split_persons('Evan Harley, Hugh MacIntosh; Meg Sugrue| Dave Stewart: test')
        self.assertEqual(['Evan Harley', 'Hugh MacIntosh', 'Meg Sugrue', 'Dave Stewart', 'test'], value)

    def test_find_taxa(self):
        value = self.impt._find_taxa()
        test_values = {'Cancer productus': [98672], 'Cancer magister': [98675]}
        self.assertEqual(test_values, value)

    def test_query_taxa(self):
        test_taxa = ['Tryphon', 'Heterocladium macounii Best', 'Trivias', 'Homoglaea hircina', 'Acanthodiaptomus']
        value = []
        test_values = [70215058, 90223816, 'NEW?', 70212504, 70219886, 70219887, 70219888,
                        70219889, 70219890, 70219898, 85836, 80047655]
        for taxon in test_taxa:
            value.extend(self.impt._query_taxa(taxon))
        self.assertEqual(test_values, value)

    def test_find_events(self):
        value = self.impt._find_events()
        self.assertEqual({}, value)

    def test_find_site(self):
        value = self.impt._find_site()
        test_values = {}
        self.assertEqual({}, value)
    
    def test_generate_site(self):
        self.fail("Not implemented")

    def test_generate_event(self):
        self.fail("Not implemented")

    def test_write_spreadsheet(self):
        self.fail("Not implemented")



if __name__ == '__main__':
    unittest.main()
