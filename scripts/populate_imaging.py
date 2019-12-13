from u19_pipeline_python import lab, reference, subject, task, action, acquisition, imaging


kargs = dict(supress_errors=True, display_progress=True)
acquisition.Scan.populate(**kargs)
