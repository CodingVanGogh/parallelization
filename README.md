# Parallelization for python2.7

This is a generic package for parallelization using Threads and Processes

##### Install using pip 

* pip install plmap

##### Install from the source
* Download the source  : git clone https://github.com/brlohith/parallelization.git
* cd parallelization
* python setup.py install

#### Example Usage

```python
def add(a, b):     # Function to be called parallely
    return a + b
```

```python
input = [ [2, 3], [4, 5], [10, 11] ]  # Set of inputs to the add function
```

* Parallelization using Multi-Threading
    ``` python
        from plmap import plmapt
        errors, outputs = plmapt(add, input, [], 3, None) 
    ```
    ``` python
        errors :  [('0', 'None'), ('0', 'None'), ('0', 'None')]
        # [ ('<error_code>', '<error_description>') ..... ]
        
        outputs : [5, 9, 21]
        # [ <output for the first set of inputs>, <output for the second set of inputs> , ..... ]
    ```
* Parallelization using Multi-Processing
    ``` python
        from plmap import plmapp
        errors, outputs = plmapp(add, input, [], 3, None) 
    ```
    ``` python
        errors :  [('0', 'None'), ('0', 'None'), ('0', 'None')]
        # [ ('<error_code>', '<error_description>') ..... ]
        
        outputs : [5, 9, 21]
        # [ <output for the first set of inputs>, <output for the second set of inputs> , ..... ]
    ```

        




