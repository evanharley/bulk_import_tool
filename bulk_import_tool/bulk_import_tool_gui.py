import wx
from bulk_import_tool import ImportTools
from pubsub import pub

'''Graphical User Interface for the Bulk Import application for Integrated Museum Management, 
Royal BC Museum's Collection Management tool'''
APP_RELOAD = 1
# begin wxGlade: dependencies
# end wxGlade

# begin wxGlade: extracode
# end wxGlade

class ImportToolsProgressDialog(wx.ProgressDialog):
    '''Progress Dialog box'''
    def __init__(self):
        """Constructor"""
        wx.ProgressDialog.__init__(self, "Processing", "Please wait...", style=wx.PD_APP_MODAL|wx.PD_AUTO_HIDE)
        self.SetSize((800, 400))
        self.count = 0 
        # create a pubsub receiver
        pub.subscribe(self.updateProgress, "UpdateMessage")

    def updateProgress(self, message, update_count=3, new_max=0):
        """"""
        if update_count == 1:
            self.count = 0
            self.SetRange(new_max)
        elif update_count == 2:
            self.SetRange(self.GetRange() + new_max)
        elif update_count == -1:
            self.count = self.GetRange()
        else:
            self.count += 1
 
        if self.count >= self.GetRange():
            self.Destroy()
 
        self.Update(self.count, message)

    def complete(self):
        self.updateProgress('COMPLETE!!!', -1)


class ToolsWindow(wx.Frame):
    ''' Main window'''
    def __init__(self, *args, **kwds):
        # begin wxGlade: tools_window.__init__
        kwds["style"] = kwds.get("style", 0) | wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwds)
        menubar = wx.MenuBar()
        fileMenu = wx.Menu()
        open = wx.MenuItem(fileMenu, wx.ID_OPEN, '&Open\tCTRL+O')
        quit = wx.MenuItem(fileMenu, wx.ID_EXIT, '&Quit\tCTRL+Q')
        
        fileMenu.Append(open)
        fileMenu.Append(quit)
        menubar.Append(fileMenu, '&File')
        
        self.Center()
        self.SetSize((400, 250))
        self.choice_1 = wx.Choice(self, wx.ID_ANY, choices=["BC Archaeology", "Botany", "Entomology", 
                                                            "Geology", "Herpetology", "Ichthyology", 
                                                            "Indigenous Collections", 
                                                            "Invertebrate Zoology",
                                                            "Mammalogy", "Modern History",
                                                            "Ornithology",
                                                            "Paleontology"])
        self.button_3 = wx.Button(self, wx.ID_ANY, "Set Discipline")
        self.button_6 = wx.Button(self, wx.ID_ANY, "Write Spreadsheet")
        self.button_7 = wx.Button(self, wx.ID_ANY, "Add IDs")
        self.button_8 = wx.Button(self, wx.ID_ANY, "Write to DB")
        self.impt = ImportTools()
        self.status = 'Please Select the Discipline'
        self.__set_properties()
        self.__do_layout()
        self.Bind(wx.EVT_MENU, self.OnQuit, quit)
        self.Bind(wx.EVT_MENU, self.OpenFile, open)
        self.Bind(wx.EVT_BUTTON, self.set_discipline, self.button_3)
        self.Bind(wx.EVT_BUTTON, self.write_spreadsheet, self.button_6)
        self.Bind(wx.EVT_BUTTON, self.add_ids, self.button_7)
        self.Bind(wx.EVT_BUTTON, self.write_to_database, self.button_8)
        self.SetMenuBar(menubar)
        # end wxGlade

    def __set_properties(self):
        # begin wxGlade: tools_window.__set_properties
        self.SetTitle("Import Tools")
        self.choice_1.SetMinSize((150, 25))
        self.button_3.SetMinSize((81, 23))
        # end wxGlade

    def __do_layout(self):
        # begin wxGlade: tools_window.__do_layout
        sizer_3 = wx.BoxSizer(wx.VERTICAL)
        sizer_6 = wx.BoxSizer(wx.HORIZONTAL)
        sizer_9 = wx.BoxSizer(wx.VERTICAL)
        sizer_8 = wx.BoxSizer(wx.VERTICAL)
        sizer_7 = wx.BoxSizer(wx.VERTICAL)
        sizer_5 = wx.BoxSizer(wx.VERTICAL)
        label_1 = wx.StaticText(self, wx.ID_ANY, "Select the process step, and input the Discipline Type",
                                style=wx.ALIGN_CENTER)
        label_1.SetMinSize((300, 30))
        sizer_3.Add(label_1, 0, wx.ALIGN_CENTER | wx.ALL, 0)

        self.label_2 = wx.TextCtrl(self, wx.ID_ANY, self.status, style = wx.TE_CENTRE | wx.TE_READONLY)
        self.label_2.SetMinSize((250,20))
        sizer_3.Add(self.label_2, 0, wx.ALIGN_CENTER | wx.ALL, 0)
        sizer_5.Add(self.choice_1, 0, wx.ALIGN_CENTER, 0)
        sizer_5.Add(self.button_3, 0, wx.ALIGN_CENTER, 0)
        sizer_3.Add(sizer_5, 0, wx.EXPAND, 0)
        label_2 = wx.StaticText(self, wx.ID_ANY, "", style=wx.ALIGN_CENTER)
        label_2.SetMinSize((100, 25))
        sizer_7.Add(label_2, 0, wx.ALIGN_CENTER, 0)
        sizer_7.Add(self.button_6, 0, wx.ALIGN_CENTER | wx.ALL, 0)
        sizer_7.Add((0, 0), 0, 0, 0)
        sizer_6.Add(sizer_7, 1, wx.EXPAND, 0)
        label_3 = wx.StaticText(self, wx.ID_ANY, "")
        label_3.SetMinSize((100, 25))
        sizer_8.Add(label_3, 0, wx.ALIGN_CENTER, 0)
        sizer_8.Add(self.button_7, 0, wx.ALIGN_CENTER | wx.ALL, 0)
        sizer_8.Add((0, 0), 0, 0, 0)
        sizer_6.Add(sizer_8, 1, wx.EXPAND, 0)
        label_4 = wx.StaticText(self, wx.ID_ANY, "")
        label_4.SetMinSize((100, 25))
        sizer_9.Add(label_4, 0, wx.ALIGN_CENTER, 0)
        sizer_9.Add(self.button_8, 0, wx.ALIGN_CENTER | wx.ALL, 0)
        sizer_9.Add((0, 0), 0, 0, 0)
        sizer_6.Add(sizer_9, 1, wx.EXPAND, 0)
        sizer_3.Add(sizer_6, 1, wx.EXPAND, 0)
        self.SetSizer(sizer_3)
        self.Layout()
        # end wxGlade

    def OnQuit(self, event):
        self.Close()

    def OpenFile(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        file_dialog = wx.FileDialog(self, "Open Template", wildcard='.xlsx Files (*.xlsx)|*.xlsx',
                                    style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        file_dialog.Center()
        file_dialog.ShowModal()
        result = self.impt._get_file(file_dialog.GetPath())
        if result[0] == -1:
            err_dlg = wx.MessageBox(result[1], 'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        file_dialog.Destroy()
        self.label_2.SetLabel(self.impt.proc_log[-1])

    def Reload(self):
        self.impt._get_file(self.impt.data_filename)
        self.impt._get_prog_info()
        self.label_2.SetLabel(self.impt.proc_log[-1])
        

    def set_discipline(self, event):  # wxGlade: tools_window.<event_handler>
        self.impt.discipline = self.choice_1.StringSelection[:3].lower()
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        if self.impt.discipline in ['bot', 'ent', 'geo', 'her', 'ich', 'inv', 'mam', 'orn', 'pal']:
            self.impt.area_cd = 'natural'
        else:
            hhdisc = {'arc': 'archeolg', 'eth': 'ethnolg', 'mod': 'history'}
            self.impt.discipline = hhdisc[self.impt.discipline]
            self.impt.area_cd = 'human'
        self.label_2.SetLabel('Load Import Spreadsheet')
        event.Skip()

    def write_spreadsheet(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        elif self.impt.ws is None:
            err_dlg = wx.MessageBox('Spreadsheet not loaded', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        else:
            prg_dlg = ImportToolsProgressDialog()
            prg_dlg.Show()
            write = self.impt.write_spreadsheet()
            if write == 0:
                dialog = wx.MessageBox('Writing Spread sheet is complete', 'Info', 
                              wx.OK | wx.ICON_INFORMATION)
                prg_dlg.complete()
            else:
                dialog = wx.MessageBox('Writing Spreadsheet failed', 'Error',
                                       wx.OK|wx.ICON_ERROR)
                prg_dlg.complete()
                self.Reload()
                event.Skip()
        self.impt._write_prog()
        self.Reload()
        event.Skip()

    def add_ids(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0

        elif self.impt.ws is None:
            err_dlg = wx.MessageBox('Spreadsheet not loaded', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        prg_dlg = ImportToolsProgressDialog()
        prg_dlg.Show()
        write, message = self.impt._add_ids()
        if write == 0:
            dialog = wx.MessageBox('Adding IDs is complete', 'Info',
                         wx.OK | wx.ICON_INFORMATION)
        else:
            dialog = wx.MessageBox('Adding IDs failed! \n {}'.format(write), 'Error',
                                   wx.OK | wx.ICON_ERROR)
        prg_dlg.Destroy()
        self.impt._write_prog()
        self.Reload()
        event.Skip()

    def write_to_database(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        elif self.impt.ws is None:
            err_dlg = wx.MessageBox('Spreadsheet not loaded', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        opt = ['Production', 'Test']
        dialog = wx.SingleChoiceDialog(self, 'Choose which datbase to write to',
                                      'Database Chooser', opt, wx.CHOICEDLG_STYLE)
        if dialog.ShowModal() == wx.ID_OK:
            value = dialog.GetStringSelection()
        else:
            return 0
        dialog.Destroy()
        if value == 'Production':
            result = self.impt._to_prod()
        else:
            result = self.impt._to_test()

        if result == -1:
            err_dlg = wx.MessageBox(result[1], 'ERROR!!', wx.OK | wx.ICON_ERROR)
        processes = ['Full Import',
                     'Write GeographicSites and Collection Events',
                     'Write Specimen, and Taxonomy Data',
                     'Update Existing Records',
                     'Write Person Data'
                     ]
        process_dlg = wx.SingleChoiceDialog(self, 'Choose the process you wish to perform',
                                           'Process Chooser', processes, wx.CHOICEDLG_STYLE)
        if process_dlg.ShowModal() == wx.ID_OK:
            process = process_dlg.GetStringSelection()
        else:
            return 0
        process_dlg.Destroy()
        prg_dlg = ImportToolsProgressDialog()
        prg_dlg.Show()
        if process == 'Full Import':
            status = self.impt.write_to_db()
        elif process == 'Write GeographicSites and Collection Events':
            status = self.impt.write_siteevent_to_db()
        elif process == 'Write Specimen, and Taxonomy Data':
            status = self.impt.write_specimen_taxa_to_db()
        elif process == 'Update Existing Records':
            status = self.impt.write_specimen_taxa_to_db(update=True)
        elif process == "Write Person Data":
            status = self.impt.write_persons_to_db()
        if status != 0:
            wx.MessageBox(status, "ERROR!", wx.OK | wx.ICON_ERROR)
            return 0
        prg_dlg.Destroy()
        self.impt._write_prog()
        event.Skip()


# end of class tools_window

class BulkImportToolGUI(wx.App):
    def OnInit(self):
        self.main_window = ToolsWindow(None, wx.ID_ANY, "")
        self.SetTopWindow(self.main_window)
        self.main_window.Show()
        return True

# end of class BulkImportToolGUI


if __name__ == "__main__":
    app = BulkImportToolGUI(0)
    app.MainLoop()
