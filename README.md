# U19-pipeline_python

The python data pipeline defined with DataJoint for U19 projects

The data pipeline is mainly ingested and maintained with the matlab repository: https://github.com/BrainCOGS/U19-pipeline-matlab

This repository is the mirrored table definitions for the tables in the matlab pipeline.

# Installation

## Prerequisites (for recommended conda installation)

1. Install conda on your system:  https://conda.io/projects/conda/en/latest/user-guide/install/index.html
2. If running in Windows get [git](https://gitforwindows.org/)
3. (Optional for ERDs) [Install graphviz](https://graphviz.org/download/)

## Installation with conda

1. Open a new terminal 
2. Clone this repository: `git@github.com:BrainCOGS/U19-pipeline_python.git`
    - If you cannot clone repositories with ssh, [set keys](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
3. Create a conda environment: `conda create -n u19_datajoint_env python==3.7`.
4. Activate environment: `conda activate u19_datajoint_env`.   **(Activate environment each time you use the project)**
5. Change directory to this repository `cd U19_pipeline_python`.
6. Install all required libraries `pip install -e .`
7. Datajoint Configuration: `jupyter notebook notebooks/00-datajoint-configuration.ipynb` 

## Tutorials

We have created some tutorial notebooks to help you start working with datajoint

1. Querying data (**Strongly recommended**) 
 - `jupyter notebook notebooks/tutorials/1-Explore U19 data pipeline with DataJoint.ipynb`

2. Building analysis pipeline (Recommended only if you are going to create new databases or tables for analysis)
- `jupyter notebook notebooks/tutorials/2-Analyze data with U19 pipeline and save results.ipynb`
- `jupyter notebook notebooks/tutorials/3-Build a simple data pipeline.ipynb`


Ephys element and imaging element require root paths for ephys and imaging data. Here are the notebooks showing how to set up the configurations properly.

- [Ephys element Configuration](notebooks/ephys_element/00-Set-up-configuration.ipynb)
- [Imaging element Configuration](notebooks/imaging_element/00-Set-up-configration.ipynb)

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

Ephys related tables were created with [DataJoint Element Array Ephys](https://github.com/datajoint/element-array-ephys), processing ephys data aquired with SpikeGLX and pre-processed by Kilosort2.  For this pipeline we are using the (acute) `ephys` module from `element-array-ephys`.

![Ephys Diagram](images/ephys_element_erd.png)


- imaging
Imaging pipeline processed with customized algorithm for motion correction and CNMF for cell segmentation in matlab.
![Imaging Diagram](images/imaging_erd.png)


- scan_element and imaging_element

Scan and imaging tables created with [DataJoint Element Calcium Imaging](https://github.com/datajoint/element-calcium-imaging), processing imaging data acquired with ScanImage and pre-processed by Suite2p.

![Scan element and imaging element Diagram](images/imaging_element_erd.png)


## Datajoint features
Import datajoint as follows:
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
