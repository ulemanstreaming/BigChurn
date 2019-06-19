'''
Developed by Data Science Elite Team, IBM Data Science and AI
- Robert Uleman - Data Science Engineer
Copyright (c) 2019 IBM Corporation
'''

import os
import sys
import uuid
import glob
import shutil
import pprint
import logging
import argparse

def spark_write_one_csv(df_out, path, mode='overwrite', compression='gzip', **kwargs):
    '''
    Write a Spark DataFrame into a single CSV file, available at the project or library level (meaning,
    not in a newly created subdirectory). By default, the CSV file will be compressed using gzip and
    contain a header; and an existing file will be overwritten.

    When you write data out in Spark (DataFrame.write.csv) you don't usually get a single file that shows up
    in the list of project assets, but a directory containing multiple files whose names start with `part-`,
    one for each of the in-memory partitions (more or less).

    Such files are easy enough to read back into a Spark DataFrame. But reading partitioned files
    into a Pandas DataFrame is difficult. Moreover, they are awkward in Watson Studio: in the Data sets
    view and the Find Data sidebar, all the separate partitions show up as individual files, along with
    unnecessary auxiliary files such as one called _SUCCESS.

    This function saves a Spark DataFrame into a single-partition CSV file and follows up with the
    operating system commands to make that file readily available as a single Watson Studio project or
    library data set. It is a wrapper around the DataFrame.write.csv() method; it keeps the same signature
    as much as possible, with the following exceptions:
        - It requires passing in the DataFrame as a parameter, since it is a function, not a method.
        - Any arguments past compression must be passed as keyword arguments; they cannot be passed
          positionally (i.e., they must identify the parameter name, as in param=...).

    Limitations
        - Memory limits may come into play when repartitioning a DataFrame into a single partition.
          Changing the Spark configuration parameters spark.driver.memory and spark.executor.memory
          can help, along with using an environment that reserves its resources.
        - There are no provisions for graceful recovery from file system out-of-space errors.
        - Appending (mode='append') to an existing file is not supported. If the file does not exist,
          this mode is allowed, as its behavior is the same as the other options.
          (The purpose of this function is to write a (large) Spark DataFrame to a single CSV file, not to
          use the file system for unioning (large) datasets; so there should be no need for append functionality.)

    Parameters
        df_out  the Spark DataFrame
        path    full path for the resulting file (including type extension, e.g., .csv or .csv.gz)

    The remaining parameters are pass-through to the pyspark.sql.DataFrameWriter interface, with the
    following overridden default values (because they are deemed desirable in most cases):
        mode='overwrite'
        compression='gzip'
        header=True
    
    Logging
        To receive logging output, use the Python logging module to create a logger called 'spark1csv'
        and set the level to logging.INFO or logging.DEBUG.
    '''

    logger = logging.getLogger('spark1csv')

    # Override the default from DataFrame.write.csv(); the other overrides are handled in the
    # function signature definition.
    header = kwargs.pop('header', True)
    
    locs = locals()
    logger.debug('Function arguments:\n%s', pprint.pformat({k:locs[k] for k in locs if k != 'logger'}))

    # Make sure we get the right behavior for the selected mode.
    # If the output file does not exist, all four modes behave the same. But if it does exist, behavior
    # depends on the mode.
    if os.path.exists(path):
        logger.info('File already exists.')
        if mode == 'append':
            raise ValueError(
                'Path {} already exists; appending to an existing file is not supported.'.format(path))
        elif mode == 'ignore':
            logger.info('File exists but mode set to "ignore". No action taken.')
            return  # Silently fail to do anything
        elif mode == 'error' or mode is None:
            raise FileExistsError('Path file:{} already exists.'.format(path))
        elif mode == 'overwrite':
            logger.info('Existing file will be overwritten.')
            pass

    # Let DataFrame.write.csv() create a temporary "file" with a unique name. This is extremely unlikely to already exist.
    temppath = os.path.join(os.path.dirname(path), str(uuid.uuid4()))
    logger.debug('Temporary path: %s', temppath)

    (df_out
     .repartition(1)  # This is important: avoid getting multiple files
     .write
     .csv(temppath, mode=mode, compression=compression, header=header, **kwargs)
     )

    # Find the partition file in the (newly created or overwritten) directory
    parts = glob.glob(os.path.join(temppath, 'part*'))
    logger.debug('Partitions: {}'.format(parts))

    # Make sure there's exactly one partition file
    if len(parts) == 1:
        part = parts[0]
        logger.debug('Single-partition output file found. Moving and renaming ...')

        # shutil.move silently overwrites existing files. That's why we have to catch a pre-existing file earlier.
        result = shutil.move(part, path)
        sizeMB = os.path.getsize(result) / (1024 * 1024.0)
        logger.info('The file %s (%.1f MB) is now available as a single CSV file.', result, sizeMB)

        # Remove the temporary partitioned file
        shutil.rmtree(temppath)

    # If there are too many or zero partitions, don't clean up the temporary file, so you can investigate the problem
    elif len(parts) == 0:
        raise FileNotFoundError(
            'No partition files found in {}. Something went wrong writing the partitioned DataFrame.'.format(temppath))
    else:  # Neither zero nor one, therefore more than one
        raise FileExistsError(
            'Found {} partition files in {}. DataFrame was not successfully reduced to a single partition.'.format(
                len(parts), temppath))

# Unit test
def main(argv):
    # Define SparkSession
    from pyspark     import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    
    # The Console Progress Bar output only makes sense in an interactive session.
    # In the log file it interferes with program log messages.
    conf  = SparkConf().set('spark.ui.showConsoleProgress', 'False')
    sc    = SparkContext(conf=conf)
    spark = SparkSession(sc).builder.getOrCreate()
    sc.setLogLevel('OFF')
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path',      help='Directory path for test output file',
                        default=os.path.join(os.environ['DSX_PROJECT_DIR'], 'datasets'))
    parser.add_argument('-f', '--filename',  help='Test output filename',
                        default='testfile')
    parser.add_argument('-e', '--extension', help='Test output filename extension',
                        default='.csv.gz', choices=['.csv', '.csv.gz'])
    parser.add_argument('-c', '--compress',  help='Compression type',
                        default='gzip', choices=['gzip', 'None'])
    parser.add_argument('-n', '--noheader',  help='Suppress the header row',
                        action='store_true')
    parser.add_argument('-r', '--rows',      help='Number of rows in test DataFrame',
                        default=1000000, type=int)
    parser.add_argument('-l', '--loglevel',  help='Verbosity of output',
                        default='INFO', choices=['DEBUG', 'INFO', 'WARN'])

    args     = parser.parse_args()
    compress = args.compress if args.compress.lower() != 'none' else None
    header   = not args.noheader
    if args.loglevel == 'DEBUG':
      level = logging.DEBUG
    elif args.loglevel == 'INFO':
      level = logging.INFO
    else:
      level = logging.WARN

    # Get ready for debugging
    logger    = logging.getLogger('spark1csv')
    handler   = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger .setLevel(level)
    logger .addHandler(handler)

    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Generate a sample DataFrame.
    # Write a million rows populated with random numeric data. You can change the number of rows.
    from pyspark.sql.functions import rand

    rows   = args.rows
    df_all = (spark.range(0, rows, numPartitions=10)
              .withColumnRenamed('id', 'ID')
              .withColumn('COLUMN_ONE'  , (rand(seed=123)*rows + 1*rows).cast('integer'))
              .withColumn('COLUMN_TWO'  , (rand(seed=456)*rows + 2*rows).cast('integer'))
              .withColumn('COLUMN_THREE', (rand(seed=789)*rows + 3*rows).cast('integer'))
              ).cache()
    
    # Use logging, not print, to make it part of the same stream (stderr) as output from the function.
    # Otherwise, output from the main (stdout) and the function come out in the wrong order
    logger.info('Number of rows:       %7d', df_all.count())
    logger.info('Number of partitions: %7d', df_all.rdd.getNumPartitions())

    outfile = os.path.join(args.path, args.filename + args.extension)

    # Experiment with different values, especially for mode
    # First write a new file
    if os.path.exists(outfile):
        os.remove(outfile)

    logger.info('--- File does not exist, mode %s ---', 'overwrite')
    try:        # File does not exist, mode overwrite: should just work
        spark_write_one_csv(df_all, outfile, mode='overwrite', compression=compress, header=header)
    except:
        logger.error('Unexpected error: ', sys.exc_info()[0])
        raise

    # File exists, mode append: should raise exception because append is not supported
    logger.info('--- File exists, mode %s ---', 'append')
    try: 
        spark_write_one_csv(df_all, outfile, mode='append'   , compression=compress, header=header)
    except ValueError as err:
        logger.info('Expected ValueError properly caught: {}'.format(err))
    except:
        logger.error('Unexpected error: ', sys.exc_info()[0])
        raise
      
    # File exists, mode ignore: silently skips writing the file
    logger.info('--- File exists, mode %s ---', 'ignore')
    try:
        spark_write_one_csv(df_all, outfile, mode='ignore'   , compression=compress, header=header)
    except:
        logger.error('Unexpected error: ', sys.exc_info()[0])
        raise

    # File exists, mode error: should raise exception
    logger.info('--- File exists, mode %s ---', 'error')
    try:
        spark_write_one_csv(df_all, outfile, mode='error'    , compression=compress, header=header)
    except FileExistsError as err:
        logger.info('Expected FileExistsError properly caught: {}'.format(err))
    except:
        logger.error('Unexpected error: ', sys.exc_info()[0])
        raise

    # File exists, mode overwrite: should just work
    logger.info('--- File exists, mode %s ---', 'overwrite')
    try:
        spark_write_one_csv(df_all, outfile, mode='overwrite', compression=compress, header=header)
    except:
        logger.error('Unexpected error: ', sys.exc_info()[0])
        raise

if __name__ == "__main__":
   main(sys.argv[1:])
