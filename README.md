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

# Accessing data files on your system
There are several data files (behavior, imaging & electrophysiology) that are referenced in the database
To access thse files you should mount PNI file server volumes on your system.
There are three main file servers across PNI where data is stored (braininit, Bezos & u19_dj)

### On windows systems
- From Windows Explorer, select "Map Network Drive" and enter: <br>
    [\\\cup.pni.princeton.edu\braininit\\]() (for braininit) <br>
    [\\\cup.pni.princeton.edu\Bezos-center\\]()     (for Bezos) <br>
    [\\\cup.pni.princeton.edu\u19_dj\\]()   (for u19_dj) <br>
- Authenticate with your **NetID and PU password** (NOT your PNI password, which may be different). When prompted for your username, enter PRINCETON\netid (note that PRINCETON can be upper or lower case) where netid is your PU NetID.
  
### On OS X systems
- Select "Go->Connect to Server..." from Finder and enter: <br>
    [smb://cup.pni.princeton.edu/braininit/]()    (for braininit) <br>
    [smb://cup.pni.princeton.edu/Bezos-center/]()    (for Bezos) <br>
    [smb://cup.pni.princeton.edu/u19_dj/]()   (for u19_dj) <br>
- Authenticate with your **NetID and PU password** (NOT your PNI password, which may be different).

### On Linux systems
- Follow extra steps depicted in this link: https://npcdocs.princeton.edu/index.php/Mounting_the_PNI_file_server_on_your_desktop

### Notable data 
Here are some shortcuts to common used data accross PNI

**Sue Ann's Towers Task**
- Imaging: [/Bezos-center/RigData/scope/bay3/sakoay/{protocol_name}/imaging/{subject_nickname}/]() 
- Behavior: [/braininit/RigData/scope/bay3/sakoay/{protocol_name}/data/{subject_nickname}/]()

**Lucas Pinto's Widefield**
- Imaging [/braininit/RigData/VRwidefield/widefield/{subject_nickname}/{session_date}/]()
- Behavior [/braininit/RigData/VRwidefield/behavior/lucas/blocksReboot/data/{subject_nickname}/]()

**Lucas Pinto's Opto inactivacion experiments**
- Imaging [/braininit/RigData/VRLaser/LaserGalvo1/{subject_nickname}/]()
- Behavior [/braininit/RigData/VRLaser/behav/lucas/blocksReboot/data/{subject_nickname}/]()

### Get path info for the session behavioral file
1. Mount needed file server
2. Connect to the Database
3. Create a structure with subject_fullname and session_date from the session <br>
```key['subject_fullname'] = 'koay_K65'``` <br>
```key['session_Date'] = '2018-02-05'``` <br>
4. Fetch filepath info:
```data_dir = (acquisition.SessionStarted & key).fetch('remote_path_behavior_file')``` <br>

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
