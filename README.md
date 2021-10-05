# U19-pipeline_python

The python data pipeline defined with DataJoint for U19 projects

The data pipeline is mainly ingested and maintained with the matlab repository: https://github.com/shenshan/U19-pipeline-matlab

This repository is the mirrored table definitions for the tables in the matlab pipeline.

# Installation

## Prerequisites

1. Install conda on your system:  https://conda.io/projects/conda/en/latest/user-guide/install/index.html
2. If in Windows OS get [git](https://gitforwindows.org/)
3. (Optional for ERDs) [Install graphviz](https://graphviz.org/download/)

## Using conda (preferred method)

3. Clone this repository: `git@github.com:BrainCOGS/U19-pipeline_python.git`
    - [Instructions for setting ssh keys in your system](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
4. Create a conda environment: `conda create -n u19_datajoint_env python==3.7`.
5. Activate environment: `conda activate u19_datajoint_env`.   **(Activate environment each time you use the project)**
6. Change directory to this repository `cd U19_pipeline_python`.
7. Install all required libraries `pip install -e .`
8. Datajoint Configuration: `jupyter notebook notebooks/00-datajoint-configuration.ipynb` 


Ephys element and imaging element require root paths for ephys and imaging data. Here are the notebooks showing how to set up the configurations properly.

[Ephys element Configuration](notebooks/ephys_element/00-Set-up-configuration.ipynb)
[Imaging element Configuration](notebooks/imaging_element/00-Set-up-configration.ipynb)


# Major schemas

Currently, the main schemas in the data pipeline are as follows:

-  lab

![Lab Diagram](images/lab_erd.png)

-  reference

![Reference Diagram](images/reference_erd.png)

- subject

![Subject Diagram](images/subject_erd.png)

- action

![Action Diagram](images/action_erd.png)

- acquisition

![Acquisition Diagram](images/acquisition_erd.png)

- task

![Task Diagram](images/task_erd.png)

- behavior

Behavior data for Towers task.

![Behavior Diagram](images/behavior_erd.png)

- ephys_element

Ephys related tables were created with [DataJoint Element Array Ephys](https://github.com/datajoint/element-array-ephys), processing ephys data aquired with SpikeGLX and pre-processed by
Kilosort2.

![Ephys Diagram](images/ephys_element_erd.png)


- imaging
Imaging pipeline processed with customized algorithm for motion correction and CNMF for cell segmentation in matlab.
![Imaging Diagram](images/imaging_erd.png)


- scan_element and imagine_element

Scan and imaging tables created with [DataJoint Element Calcium Imaging](https://github.com/datajoint/element-calcium-imaging), processing imaging data acquired with Scan Image and pre-processed by Suite2p.

![Scan element and imaging element Diagram](images/imaging_element_erd.png)


## Undocumented datajoint features
For all code below, I am assuming datajoint has been imported like:
```python
import datajoint as dj
```

### Update a table entry
`dj.Table._update(schema.Table & key, 'column_name', 'new_data')`

### Get list of all column names in a table (without having to issue a query or fetch)
`table.heading.attributes.keys()`

This also works on a query object:
```python
schema = dj.create_virtual_module("some_schema","some_schema")
query_object = schema.Sample() & 'sample_name ="test"'
query_object.heading.attributes.keys()
```

The latter case is useful if you are passing the query object between functions or modules and you lose track of the table name.

### Use boolean datatype
Example table:
```
@schema
class Experiment(dj.Manual):
    definition = """ # Experiments performed using the light sheet microscope
    experiment_id           :   smallint auto_increment    # allowed here are sql datatypes.
    ----
    cell_detection          :   boolean

    """
```
It has some counterintuitive properties:

| Inserted_value      | Stored_value |
| ----------- | ----------- |
| True      | 1       |
| False   | 0        |
| 1   | 1        |
| 0   | 0        |
| 5   | 5*        |
| -5   | -5*        |
|5000  | DataError* |
|-5000  | DataError* |
|'10'  | 10* |
|'-10'  | -10* |
|'0'    | 0*    |

\*Would expect this to be stored as 1 based on the rules of `bool` in python. See: https://github.com/datajoint/datajoint-docs/issues/222
