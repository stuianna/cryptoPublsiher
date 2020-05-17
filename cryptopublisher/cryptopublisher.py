import logging
import appdirs
import os
import time
import argparse
import socket
import subprocess
from cryptopublisher._version import __version__
import dbops.timeconverter as timeconverter
from dbops.influxhelper import InfluxHelper

log = logging.getLogger(__name__)
PUBLISHER_NAME = 'cryptoPublisher'
MAXIMUM_UPDATE_SIZE = 10000


class CryptoPublisher():
    def setup_logging(logLevel, directory, output_file=None):
        full_path = os.path.join(directory, output_file)
        logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s: %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S%p',
                            level=logLevel,
                            filename=full_path)

    def get_working_directory(publisher_name):
        dir_path = os.path.join(appdirs.user_config_dir(PUBLISHER_NAME), publisher_name)
        return CryptoPublisher.create_dir_if_not_exit(dir_path)

    def create_dir_if_not_exit(dir_path):
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except OSError as e:
                log.error("Cannot create required directory. Excpetion {}".format(e))
                return None
        return dir_path

    def setup_influx_database(db_name):
        influx = InfluxHelper(db_name)
        if influx.exists():
            return influx
        else:
            return None

    def reset_influx_database(influx, db_name):
        influx.remove_database(db_name)
        influx = CryptoPublisher.setup_influx_database(db_name)

    def remove_influx_db_measurements(influx, symbols):
        log.warning("Removing influx database measurements")
        for symbol in symbols:
            if not influx.remove_measurement(symbol):
                log.error("Failed to remove measurement {}".format(symbol))

    def get_fiends_and_drop_na(df, fields):
        extra_entries = False
        if len(df) > MAXIMUM_UPDATE_SIZE:
            extra_entries = True

        df = df.loc[0:MAXIMUM_UPDATE_SIZE-1, fields + ['timestamp']]
        na_entries = df.isnull().sum().sum()
        if na_entries > 0:
            log.warning("Dropping {} na entries".format(na_entries))
        return df.dropna(), extra_entries

    def measurement_exists(influx, measurement):
        all_measurements = influx.get_measurement_names()
        return measurement in all_measurements

    def get_last_influx_timestamp(influx, measurement, field, tags=None, tag_filter=None):
        if CryptoPublisher.measurement_exists(influx, measurement):
            last_influx_timestamp = influx.get_last_time_entry(measurement, field, tags, tag_filter, as_unix=True)
            if last_influx_timestamp is None:
                last_influx_timestamp = 0
            else:
                # Add one to timestamp, otherwise the last value in the DB is always returned
                last_influx_timestamp = last_influx_timestamp['time'] + 1
        else:
            last_influx_timestamp = 0
        log.debug("Last timestamp for {}: {}".format(measurement, timeconverter.unix_to_rfc3339(last_influx_timestamp)))
        return last_influx_timestamp

    def get_new_sqlite_entries(sqlite, table, column_filter, timestamp):
        new_entries = sqlite.get_row_range(table, 'timestamp', timestamp, int(time.time()))
        if new_entries is None:
            return None
        new_entries, extra_entries = CryptoPublisher.get_fiends_and_drop_na(new_entries, column_filter)
        return new_entries, extra_entries

    def create_common_arguments():
        parser = argparse.ArgumentParser()

        parser.add_argument('-w',
                            '--working_directory',
                            nargs=1,
                            help="Specify the directory for configuration and logs."
                            "If not supplied, set to user configuration directory.",
                            default=None)

        parser.add_argument('-k', '--kill', action='store_true', help="Kills any running instance", default=False)

        parser.add_argument('-g',
                            '--generate_config',
                            action='store_true',
                            help="Generates the neccessary configuration files and directories.")

        parser.add_argument('-l',
                            '--log',
                            nargs=1,
                            help="Log level. Must be one of either DEBUG,"
                            "INFO, WARNING, ERROR or CRITICAL. Default = INFO",
                            default="INFO")

        parser.add_argument('-c', '--clean', action='store_true', help="Remove (clean) the target influx database.")

        parser.add_argument('-v',
                            '--version',
                            action='version',
                            version="Crypto publisher collection. Version {}".format(__version__))
        return parser

    def process_log_level(level):

        if level == 'DEBUG':
            return logging.DEBUG
        elif level == 'INFO':
            return logging.INFO
        elif level == 'WARNING':
            return logging.WARNING
        elif level == 'ERROR':
            return logging.ERROR
        elif level == 'CRITICAL':
            return logging.CRITICAL
        else:
            return logging.INFO

    def already_running(process_name):
        processName = process_name
        CryptoPublisher.already_running._lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            CryptoPublisher.already_running._lock_socket.bind('\0' + processName)
            log.info("New publisher instance started")
            return False
        except Exception as e:
            _ = e
            log.warning("Attempting to start daemon which is already running")
            return True

    def kill(process_name):
        subprocess.Popen(['killall', process_name])
