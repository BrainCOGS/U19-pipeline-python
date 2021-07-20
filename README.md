# U19-pipeline_python

The python data pipeline defined with DataJoint for U19 projects

The data pipeline is mainly ingested and maintained with the matlab repository: https://github.com/shenshan/U19-pipeline-matlab

This repository is the mirrored table definitions for the tables in the matlab pipeline.


## Set up the configuration

DataJoint database credentials could be configured in the global variable `dj.config` and saved in configuration file `dj_local_conf.json` at the root of the repository folder.

```json
{
    "database.host": "datajoint00.pni.princeton.edu",
    "database.user": "<username>",
    "database.password": "<password>",
    "loglevel": "INFO",
    "safemode": true,
    "display.limit": 7,
    "display.width": 14,
    "display.show_tuple_count": true,
    "stores": {
        'extstorage':
        {
            'location': '/Volumes/u19_dj/external_dj_blobs/', # varies across different operating systems.
            'protocol': 'file'
        }
    }
}
```
`stores` specifies the external storage information. DataJoint external fields save links to data saved on the harddrive.
The `location` of `extstorage` is the filepath to the external storage, which varies for different operating systems.
For MacOS: '/Volumes/u19_dj/external_dj_blobs/',
For Windows: '\\\\bucket.pni.princeton.edu\\u19_dj\\external_dj_blobs\\'
For Linux: '/mnt/bucket/u19_dj/external_dj_blobs/'

Ephys element and imaging element require root paths for ephys and imaging data. Here are the notebooks showing how to set up the configurations properly.

[Ephys element Configuration](notebooks/ephys_element/00-Set-up-configuration.ipynb)
[Imaging element Configuration](notebooks/imaging_element/00-Set-up-configration.ipynb)


## Installation of package for usage and development.

To use and contribute to the developement of the package, we recommend either using a Docker setup or creating a virtual environment, as follows:

1. In either way, we first fork from this current repo

2. Clone the repository on your fork `git clone https://github.com/BrainCOGS/U19-pipeline_python`

3a. To use a conda environment setup (recommended), we could

> * Create a conda environment by `conda create -n u19_datajoint python==3.7`

> * With the virtual environment, we could install the package that allows edits: `pip install -e .`

3b. To use a docker setup, after installing docker, inside this directory, we

> *  set up the `.env` file, as follows:
```
DJ_HOST = 'datajoint00.pni.princeton.edu'
DJ_USER = {your_user_name}
DJ_PASSWORD = {your_password}
```
> *  run `docker-compose up -d`

> * Then, we could run `docker exec -it u19_pipeline_python_datajoint_1 /bin/bash`
This will provide you a mini environment to work with python.

## Major schemas

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
