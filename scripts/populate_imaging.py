from u19_pipeline_python import acquisition

kargs = dict(supress_errors=True, display_progress=True)
acquisition.Scan.populate(**kargs)
