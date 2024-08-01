import sys

if __name__ == '__main__':
    
    import time

    from scripts.conf_file_finding import try_find_conf_file
    try_find_conf_file()
    time.sleep(1)

    import u19_pipeline.automatic_job.pupillometry_handler as ph

    args = sys.argv[1:]
    print(args)
    print(args[0])
    print(args[1])
    print(args[2])

    ph.PupillometryProcessingHandler.analyze_videos_pupillometry(args[0], args[1], args[2])
