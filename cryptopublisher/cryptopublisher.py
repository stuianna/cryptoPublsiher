import logging
import appdirs
import os
import time
import dbops.timeconverter as timeconverter

log = logging.getLogger(__name__)
PUBLISHER_NAME = 'cryptoPublisher'


class CryptoPublisher():
    def setup_logging(logLevel, directory, output_file=None):
        full_path = os.path.join(directory, output_file)
        logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s: %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S%p',
                            level=logLevel,
                            filename=full_path)

    def get_working_directory(publisher_name):
        dir_path = os.path.join(appdirs.user_config_dir(PUBLISHER_NAME), publisher_name)
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except OSError as e:
                log.error("Cannot create required directory. Excpetion {}".format(e))
                return None
        return dir_path

    def remove_influx_db_measurements(influx, symbols):
        log.warning("Removing influx database measurements")
        for symbol in symbols:
            if not influx.remove_measurement(symbol):
                log.error("Failed to remove measurement {}".format(symbol))

    def get_fiends_and_drop_na(df, fields):
        df = df.loc[:, fields + ['timestamp']]
        na_entries = df.isnull().sum().sum()
        if na_entries > 0:
            log.warning("Dropping {} na entries".format(na_entries))
        return df.dropna()

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
        new_entries = CryptoPublisher.get_fiends_and_drop_na(new_entries, column_filter)
        return new_entries
