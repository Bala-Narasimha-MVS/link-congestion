Steps to run link congestion code by installing it as a package

1. Clone the code 
2. Go to link_congestion folder in the terminal 
3. python3 -m pip install Cython 
4. Poetry build
5. python3 -m pip install dist/link_congestion-0.1.0-cp38-cp38-manylinux_2_31_x86_64.whl   # the file name can change so install the whl file

To run the code follow the steps below
1. Copy the config file to a new folder 
2. Open terminal in that folder 
3. type linker-csv and perss enter. This will genrate teh linker.csv file
4. type link-congestion and press enter. This will run the link-congestion code.
5. If we have to run the code in backgroud use nohup.

If the are changes to the code
1. git clone 
2. python3 -m pip install dist/link_congestion-0.1.0-cp38-cp38-manylinux_2_31_x86_64.whl --force-reinstall
3. Run the code


 Steps to run by just using poetry without installing hte package
1. Clone the code 
2. Go to link_congestion folder in the terminal\
3. poetry shell
4. poetry install 
5. poetry run linker-csv # for genarating csv file 
6. poetry run link-congestion # for running the code 




