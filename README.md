# cloud-mincemeat-titanic-analysis
GUI-based Python program to generate Kaggle Titanic submission files using map-reduce on AWS-EMR or locally  

 Author John Soper
  Comments and questions welcome:  
      http://www.linkedin.com/in/johnwsoper
      micej_soper@sbcglobal.net (to email, chase out the rats)


  This Python project was written as a final project for my UCSC Cloud Computing class.
  It automates a simple map-only Hadoop Streaming run on Amazon Web Service's 
  Elastic Map Reduce (AWS-EMR).  The output file is a CSV file of Titanic survival
  prediction based on passenger characteristics and can be submitted into the 
  Kaggle.com Titanic Machine Learning competition.
  
  The project uses an optional GUI interface with the Tkinter library.  Not 
  the greatest graphics package, but being part of the standard python library
  really simplifies things.
  
  After the class was over, it was expanded with features and test code.  This was 
  both for learning and my Github portfolio.
  
FEATURES  
    1.  Runs entirely with Python and verifed on MacOS 10.7.5, Windows 7, 
        and Ubuntu 14.04  
    2.  Control panel provided by Tkinter graphics  
    3.  AWS access only requiring a .boto file in the home directory  
    4.  Unique bucket name generated from network card MAC address  
    5.  Two data processing models supported by dynamic mapper file updating  
    6.  Streaming AWS progress updates in window  
    7.  Local functional test option using Mincemeat import (a Python-only 
        mapreduce program)  
    8.  Unit tests  
    9.  Nearly full 10/10 compliance with Pylint (except for third-party Mincemeat
        module)   
    
INSTRUCTIONS   
    NOTE: All below can be run from command line or Eclipse
    
    gui_titanic  - run for AWS-EMR session with GUI (average time 8 minutes)  
    emr_titanic - run for same results as above without GUI (defaults only)  
    launch_mr_func_test - run for local map-reduce with Python Mincemeat  
    titanic_unit_tests - run for unit testing of emr_titanic.py  
    
  
DISCLAIMERS  
1.  The word "test" is overloaded in this project, which may cause confusion.
    The input file is "test.csv" because it used a model calculated elsewhere from
    "train.csv".  These are standard data science names.  The mincemeat code (including the launch file) runs a non-cloud map-reduce
    which is considered a functional test for checking the cloud results
    There's also one file of unit tests.  
 
2.  The mincemeat module and the concept of a python map-reduce are not my
    work but Michael Fairley's.  The mincemeat.py module has not been altered.  
    
    https://github.com/michaelfairley/mincemeatpy  
    
    Another good site:  
    http://mjtoolbox.wordpress.com/2013/04/21/map-reduce-using-python-mincemeat-i/  
      
3.  The complex Tkinter code in gui_titanic.py is not my work, but is from  
    username dazza at:  
    http://www.executionunit.com/blog/2012/10/26/using-python-and-tkinter-capture-script-output/  
   
    However, I did learn a lot from altering it for my needs.    
    A sincere thank you to both gentlemen mentioned above  
