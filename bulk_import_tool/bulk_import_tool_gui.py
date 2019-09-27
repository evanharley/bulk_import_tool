import wx
from bulk_import_tool import ImportTools
from pubsub import pub


# begin wxGlade: dependencies
# end wxGlade

# begin wxGlade: extracode
# end wxGlade

class ImportToolsProgressDialog(wx.Dialog):
        def __init__(self):
            """Constructor"""
            wx.Dialog.__init__(self, None, title="Progress")
            self.SetSize((400, 200))
            self.count = 0
            self.max = 0
            self.message = wx.TextCtrl(self, wx.ID_ANY, 'Please Wait...', style = wx.TE_READONLY)
            self.progress = wx.Gauge(self, range=4)
            self.progress.SetMaxSize((400, 50))
            sizer = wx.BoxSizer(wx.VERTICAL)
            sizer.Add(self.message, 0, wx.ALIGN_CENTRE)
            sizer.Add(self.progress, 1, wx.EXPAND)
            self.SetSizer(sizer)
 
            # create a pubsub receiver
            pub.subscribe(self.updateProgress, "UpdateMessage")

        def updateProgress(self, arg1, arg2=0, arg3=0):
            """"""
            if arg2 == 1:
                self.count = 0
                self.max = arg3
            else:
                self.count += 1
            self.message.SetValue(arg1)
 
            if self.count >= self.max:
                self.Destroy()
 
            self.progress.SetValue(self.count)

class ToolsWindow(wx.Frame):
    def __init__(self, *args, **kwds):
        # begin wxGlade: tools_window.__init__
        kwds["style"] = kwds.get("style", 0) | wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwds)
        self.Center()
        self.SetSize((400, 200))
        self.choice_1 = wx.Choice(self, wx.ID_ANY, choices=["Botany", "Entomology", "Geology", "Herpetology",
                                                            "Ichthyology", "Invertebrate Zoology",
                                                            "Mammalogy", "Ornithology"])
        self.button_3 = wx.Button(self, wx.ID_ANY, "Set Discipline")
        self.button_6 = wx.Button(self, wx.ID_ANY, "Write Spreadsheet")
        self.button_7 = wx.Button(self, wx.ID_ANY, "Add IDs")
        self.button_8 = wx.Button(self, wx.ID_ANY, "Write to DB")
        file_dialog = wx.FileDialog(self, "Open Template", wildcard='.xlsx Files (*.xlsx)|*.xlsx',
                                    style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        file_dialog.Center()
        file_dialog.ShowModal()
        self.impt = ImportTools()
        self.impt._get_file(file_dialog.GetPath())
        self.impt._get_prog_info()
        file_dialog.Destroy()
        if self.impt.proc_log != []:
            self.status = self.impt.proc_log[-1]
        else:
            self.status = 'New Import'
        self.__set_properties()
        self.__do_layout()
        self.Bind(wx.EVT_BUTTON, self.set_discipline, self.button_3)
        self.Bind(wx.EVT_BUTTON, self.write_spreadsheet, self.button_6)
        self.Bind(wx.EVT_BUTTON, self.add_ids, self.button_7)
        self.Bind(wx.EVT_BUTTON, self.write_to_database, self.button_8)
        # end wxGlade

    def __set_properties(self):
        # begin wxGlade: tools_window.__set_properties
        self.SetTitle("Import Tools")
        self.choice_1.SetMinSize((66, 25))
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
        label_1.SetMinSize((257, 30))
        sizer_3.Add(label_1, 0, wx.ALIGN_CENTER | wx.ALL, 0)

        label_2 = wx.TextCtrl(self, wx.ID_ANY, self.status, style = wx.TE_READONLY)
        sizer_3.Add(label_2, 0, wx.ALIGN_CENTER | wx.ALL, 0)
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

    def set_discipline(self, event):  # wxGlade: tools_window.<event_handler>
        self.impt.discipline = self.choice_1.StringSelection[:3].lower()
        if self.impt.discipline in ['bot', 'ent', 'geo', 'her', 'ich', 'inv', 'mam', 'orn', 'pal']:
            self.impt.area_cd = 'natural'
        else:
            self.impt.area_cd = 'human'
        event.Skip()

    def write_spreadsheet(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        else:
            prg_dlg = ImportToolsProgressDialog()
            prg_dlg.max = 4
            prg_dlg.Show()
            write = self.impt.write_spreadsheet()
            if write == 0:
                dialog = wx.MessageBox('Writing Spread sheet is complete', 'Info', 
                              wx.OK | wx.ICON_INFORMATION)
        self.impt._write_prog()
        event.Skip()

    def add_ids(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        prg_dlg = ImportToolsProgressDialog()
        prg_dlg.Show()
        prg_dlg.max = 4
        write, message = self.impt._add_ids()
        if write == 0:
            dialog = wx.MessageBox('Adding IDs is complete', 'Info',
                         wx.OK | wx.ICON_INFORMATION)
        else:
            dialog = wx.MessageBox('Adding IDs failed! \n {}'.format(write), 'Error',
                                   wx.OK | wx.ICON_ERROR)
        self.impt._write_prog()
        event.Skip()

    def write_to_database(self, event):
        if self.impt.discipline == '':
            err_dlg = wx.MessageBox('Discipline not selected', 
                                    'ERROR!!', wx.OK | wx.ICON_ERROR)
            return 0
        opt = ['Production', 'Test']
        dialog = wx.SingleChoiceDialog(self, 'Choose which datbase to write to',
                                      'Database Chooser', opt, wx.CHOICEDLG_STYLE)
        if dialog.ShowModal() == wx.ID_OK:
            value = dialog.GetStringSelection()
        dialog.Destroy()
        if value == 'Production':
            self.impt._to_prod()
        else:
            self.impt._to_test()
        processes = ['Full Import',
                     'Write GeographicSites and Collection Events',
                     'Write Specimen, Person and Taxonomy Data',
                     'Update Existing Records',
                     'Write Person Data'
                     ]
        process_dlg = wx.SingleChoiceDialog(self, 'Choose the process you wish to perform',
                                           'Process Chooser', processes, wx.CHOICEDLG_STYLE)
        if process_dlg.ShowModal() == wx.ID_OK:
            process = process_dlg.GetStringSelection()
        else:
            wx.MessageVox('Select a Process', "Error", wx.OK | wx.ICON_ERROR)
        process_dlg.Destroy()
        prg_dlg = ImportToolsProgressDialog()
        prg_dlg.Show()
        if process == 'Full Import':
            status = self.impt.write_to_db()
        elif process == 'Write GeographicSites and Collection Events':
            status = self.impt.write_siteevent_to_db()
        elif process == 'Write Specimen, Person and Taxonomy Data':
            status = self.impt.write_specimen_taxa_persons_to_db()
        elif process == 'Update Existing Records':
            status = self.impt.write_specimen_taxa_persons_to_db(update=True)
        elif process == "Write Person Data":
            status = self.impt.write_persons_to_db()
        if status != 0:
            wx.MessageBox(status, "ERROR!", wx.OK | wx.ICON_ERROR)
            return 0
        self.impt._write_prog()
        self.impt._get_prog_info()
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
