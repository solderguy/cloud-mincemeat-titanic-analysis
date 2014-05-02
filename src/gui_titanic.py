#!/usr/bin/env python

"""
CREDENTIALS
  Module: gui_titanic.py
  Author: John Soper (with caveats to original source, see below)
  Date: Mar 24, 2014
  Rev: 1

SUMMARY
  This program use the standard Tkinter library to provide a GUI for setting up
  and launching a Hadoop map-only job on the AWS EMR cluster

  It encodes the users run-time options and starts the emr-titanic module using
  them.  It's only a convenience program, use is not required

RESOURCES
  Original source shamelessly stolen from (combine following the 2 lines):
    http://www.executionunit.com/en/blog/2012/10/26/
	using-python-and-tkinter-capture-script-output/
  Main tutorial used:
    http://zetcode.com/gui/tkinter/
"""

import subprocess
import sys

from Tkinter import Tk, Text, BOTH, W, N, E, S, END, INSERT, HORIZONTAL, \
    VERTICAL, NONE, StringVar
                # Tk class - used to create a root window

from ttk import Frame, Button, Style, Scrollbar, Checkbutton, OptionMenu
                # Frame class - container for other widgets.
from os.path import join, dirname
from datetime import datetime


MAIN_PATH = join(dirname(__file__), "emr_titanic.py")

#there has got to be a better way to do this in windows.
PYTHON_PATH = "python"
if sys.platform == "win32":
    PYTHON_PATH = "c:\python27\python.exe" # pylint: disable=W1401

# Inherit from Frame container widget
# Pylint complains below about too many ancestors and public methods
class AssetBuilder(Frame):   # pylint: disable=R0901,R0904
    """ docstring placeholder"""

    def __init__(self, parent):	      	# constructor
        Frame.__init__(self, parent)  	# call superclass constructor
        self.parent = parent		    # save a ref to the parent widget
        #   (Tk root window)
        self.textarea = None
        self.verbose_flag = StringVar()
        self.model = StringVar()
        self.region = StringVar()
        self.init_ui()       		# delegate creation of user interface


    def init_ui(self):
        """ Initializes the GUI Tkinter window"""
        # window title
        self.parent.title("Titanic Mortality Prediction Analysis")
        #self.style = Style()
        style = Style()

        #self.style.theme_use("default")
        # has better dropdown arrows than default
        style.theme_use("clam")

        self.pack(fill=BOTH, expand=1)
        # pack() is one of the three geometry managers in Tkinter.
        # It organizes widgets into horizontal and vertical boxes.
        # Here we put the Frame widget, accessed via the self attribute
        # to the Tk root window. It is expanded in both directions.
        # In other words, it takes the whole client space of the
        # root window.

        # create a grid 3x6 in to which we will place elements.
        self.setup_grid()

        # create the main text area with scrollbars
        self.create_scrollable_text_area()

        # create the buttons/checkboxes to go along the bottom
        self.setup_widgets()

        # tags are used to colorize the text added to the text widget.
        # see self.addTtext and self.tags_for_line
        assert self.textarea is not None, 'textarea not set before use'
        self.textarea.tag_config("errorstring", foreground="#CC0000")
        self.textarea.tag_config("infostring", foreground="#008800")

        self.display_intro()

    def display_intro(self):
        """ Writes explanatory text for the user"""
        self.add_text(" This program sets up and performs a data analysis to" \
            " predict Titanic and survivors\n")
        self.add_text(" based on passenger characteristics.  It is from a " \
            "www.kaggle.com competition\n")
        self.add_text("\n")
        self.add_text(" It does this by running a hadoop map-only streaming " \
            "Python script on the AWS\n")
        self.add_text(" Elastic Map Reduce web service\n")
        self.add_text("\n")
        self.add_text(" Settings:\n")
        self.add_text("   Clear - clear this screen\n")
        self.add_text("   Silent - do not display logs, you usually want " \
            "verbose\n")
        self.add_text("   Model - which prediction model to use in order of " \
            "increasing complexity\n")
        self.add_text("   Region - which EMR server to use, currently set to " \
            "US choices only\n")
        self.add_text("   RunEMR - start\n")
        self.add_text("\n")
        self.add_text(" The size of the final output file (a 3-column csv " \
            "file) is displayed on the screen\n")
        self.add_text(" The fields are passenger_id, prediction(0=died," \
            "1=survived), and gender\n")
        self.add_text("\n")
        self.add_text(" Don't worry about 7+ minutes of STARTING messages, " \
            "AWS is setting up.  Sometimes when\n")
        self.add_text(" running on windows, the messages doen't update till " \
            "the end\n")

    def setup_grid(self):
        """Control buttons will need a grid to lay in"""
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=0)
        self.columnconfigure(3, weight=0)
        self.columnconfigure(4, weight=0)
        self.columnconfigure(5, weight=0)
        self.columnconfigure(6, weight=0)
        self.rowconfigure(1, weight=1)
        self.rowconfigure(2, weight=0)
        self.rowconfigure(3, weight=0)

    def create_scrollable_text_area(self):
        """ Creates main text area with scrollbars"""
        xscrollbar = Scrollbar(self, orient=HORIZONTAL)
        xscrollbar.grid(row=2, column=1, columnspan=5, sticky=E + W)
        yscrollbar = Scrollbar(self, orient=VERTICAL)
        yscrollbar.grid(row=1, column=6, sticky=N + S)
        self.textarea = Text(self, wrap=NONE, bd=0,
                             xscrollcommand=xscrollbar.set,
                             yscrollcommand=yscrollbar.set)
        self.textarea.grid(row=1, column=1, columnspan=5, rowspan=1,
                            padx=0, sticky=E + W + S + N)
        xscrollbar.config(command=self.textarea.xview)
        yscrollbar.config(command=self.textarea.yview)

    def setup_widgets(self):
        """Five control widgets created along bottom"""

        # clear button
        clear_button = Button(self, text="Clear")
        clear_button.grid(row=3, column=1, padx=5, pady=5, sticky=W)
        clear_button.bind("<ButtonRelease-1>", self.clear_text)

        # Silent/Verbose select
        verbose_check = Checkbutton(self, text="Silent", \
            variable=self.verbose_flag, onvalue="-s", offvalue="")
        verbose_check.grid(row=3, column=2, padx=5, pady=5)

        # Analysis model menu
        choices2 = ['Select Model', 'model1', 'model2']
        om2 = OptionMenu(self, self.model, *choices2) # pylint: disable=W0142
        om2.grid(row=3, column=3, padx=10, pady=20)

        # EMR Server Region menu
        choices = ['Select Region', 'Oregon', 'California', 'Virginia']
        om1 = OptionMenu(self, self.region, *choices) # pylint: disable=W0142
        om1.grid(row=3, column=4, padx=10, pady=20)

        # Run EMR button
        emrbutton = Button(self, text="Run EMR")
        emrbutton.grid(row=3, column=5, padx=5, pady=5)
        emrbutton.bind("<ButtonRelease-1>", self.run_emr)

    @staticmethod
    def tags_for_line(line):
        """return a tuple of tags to be applied to the line of text 'line'
           when being added to the text widget"""
        lc_line = line.lower()
        if "error" in lc_line or "traceback" in lc_line:
            return ("errorstring", )
        return ()

    def add_text(self, text_str, tags=None):
        """Add a line of text to the textWidget. If tags is None then
        self.tags_for_line will be used to assign tags to the line"""
        assert self.textarea is not None, 'textarea not set before use'
        self.textarea.insert(INSERT, text_str, tags or \
            self.tags_for_line(text_str))
        self.textarea.yview(END)

    def clear_text(self, _): #arg2 was "event", changed for pylint
        """Clear all the text from the text widget"""
        assert self.textarea is not None, 'textarea not set before use'
        self.textarea.delete("1.0", END)
        #print os.environ['AWS_ACCESS_KEY_ID']

    def move_cursor_to_end(self):
        """move the cursor to the end of the text widget's text"""
        assert self.textarea is not None, 'textarea not set before use'
        self.textarea.mark_set("insert", END)


    def run_emr(self, _): #arg2 was "event", changed for pylint
        """callback from the run EMR button"""
        self.move_cursor_to_end()
        self.add_text("Build Started %s\n" % (str(datetime.now())), \
            ("infostring", ))

        cmdlist = [x for x in [PYTHON_PATH, MAIN_PATH,
            self.verbose_flag.get(), self.model.get(), self.region.get()]
            if x]

        self.add_text(" ".join(cmdlist) + "\n", ("infostring", ))

        proc = subprocess.Popen(cmdlist,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 universal_newlines=True)

        while True:
            line = proc.stdout.readline()
            if not line:
                break
            self.add_text(line)
            # this triggers an update of the text area, otherwise it
            # doesn't update
            #self.textarea.update_idletasks()
            assert self.textarea is not None, 'textarea not set before use'
            self.textarea.update()

        self.add_text("EMR Run Finished %s\n" % (str(datetime.now())), \
		 ("infostring", ))
        self.add_text("*" * 80 + "\n", ("infostring", ))


def main():
    """ Tkinter GUI program flow"""
    root = Tk()  	# create root window (the main application window)
            # has a title bar and borders provided by the
            # window manager
            # it must be created before any other widgets.
    root.geometry("650x400+300+300")
                    # window width
                    # window height
                    # X screen coordinate
                    # Y screen coordinate

    AssetBuilder(root)		# create instance of application class

    root.mainloop() 		# enter mainloop and start event handling
                # receives events from the window system and
                # dispatches them to the application widgets.


# execute when run directly (not imported)
if __name__ == '__main__':
    main()
