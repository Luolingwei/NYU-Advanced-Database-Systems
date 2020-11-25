# NYU-Advanced-Database-Systems

Replicated Concurrency Control and Recovery (RepCRec)  

## Team members:  
Lingwei Luo, Net ID: ll4123  
Yuyang Fan, Net ID: yf1357

## How to run this program  
First please make sure you have Python 3.6+ installed, our program provide 2 mode in which you can run it.  

You should use the following command to run the program 
```python
python3 main.py [input_path]
```
* 1 The ```input_path``` is provided. ```input_path``` is the path where your test cases are, our program will run test cases one by one under this path, and results for each test case will go to the standard output. If you want to run the 20 default test cases defined by ourself, you can simply type in "" as ```input_path```.
* 2 The ```input_path``` is NOT provided. You will need to enter your test case line by line in the standard input. Enter ```exit``` to exit the program.

## How to reprozip and reprounzip
Before you do reprozip/reprounzip, please make sure you have these 2 python libs installed correctly.
* 1 reprozip  

    In your virtual machine, enter your folder where there are all files you want to pack. Run the follwing commands.
    ```python
    reprozip trace python3 main.py ""
    reprozip pack [Pack_Name]
    ```
    You will see a rpz file then.

* 2 reprounzip
    
    In the folder where there is your rpz file, run the follwing command to unzip it.
    ```python
    reprounzip directory setup [Pack_Name].rpz ./unzip
    reprounzip directory run ./unzip
    ```
    You will see a unzip folder then.

