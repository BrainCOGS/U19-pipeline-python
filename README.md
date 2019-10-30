# U19-pipeline_python
The python data pipeline defined with DataJoint for U19 projects

## Undocumented datajoint features

### Update a table entry
`dj.Table._update(schema.Table & key, 'column_name', 'new_data')`

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