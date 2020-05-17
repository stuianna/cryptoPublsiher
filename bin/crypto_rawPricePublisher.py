#!/usr/bin/env python3

import logging
import appdirs
import sys
import os
import time
from configchecker import ConfigChecker
from dbops.influxhelper import InfluxHelper
from cryptopublisher.cryptopublisher import CryptoPublisher as CryptoPublisher
from cmclogger.cmclogger import CMCLogger
import cmclogger.settings as CMCSetting

log = logging.getLogger(__name__)
PUBLISHER_NAME = 'rawPriceDataWriter'
DEFULT_TARGET_SYMBOLS = "BTC ETH LTC XRP USDT"
DEFULT_TARGET_METRICS = "price volume_24h percent_change_1h percent_change_24h percent_change_7d"
DEFAULT_INFLUX_DB_NAME = 'raw_crypto_price_data'
DEFAULT_INTERVAL_SECONDS = 60
CMC_DIRECTORY = None
LOG_LEVEL = logging.INFO


def setup_cmc(target_directory):
    if target_directory is None:
        working_directory_cmc = appdirs.user_config_dir(CMCSetting.appNameDirectory)
    else:
        working_directory_cmc = target_directory

    try:
        cmc = CMCLogger(working_directory_cmc, LOG_LEVEL)
    except Exception as e:
        log.critical("Cannot setup CMCLogger, exception {}".format(e))
        return None
    return cmc


def setup_sqlite_database(cmc):
    if type(cmc) is not CMCLogger:
        return None
    return cmc.get_database()


def setup_influx_database(db_name):
    influx = InfluxHelper(db_name)
    if influx.exists():
        return influx
    else:
        return None


def setup_configuration(working_directory):
    config = ConfigChecker()
    config.set_expectation('Settings', 'target_symbols', str, DEFULT_TARGET_SYMBOLS)
    config.set_expectation('Settings', 'target_metrics', str, DEFULT_TARGET_METRICS)
    config.set_expectation('Settings', 'update_interval_seconds', int, DEFAULT_INTERVAL_SECONDS)
    config.set_expectation('Settings', 'influx_db_name', str, DEFAULT_INFLUX_DB_NAME)

    config_file = os.path.join(working_directory, 'config.ini')
    config.set_configuration_file(config_file)
    config.write_configuration_file()
    return config


def get_symbols_and_metrics(config):
    try:
        symbols = config.get_value("Settings", 'target_symbols').split(' ')
    except Exception as e:
        log.critical("Could not interpret symbols from configuration, Symbols = {}, Exception {}".format(symbols, e))
        sys.exit(1)

    try:
        metrics = config.get_value("Settings", 'target_metrics').split(' ')
    except Exception as e:
        log.critical("Could not interpret metrics from configuration, Metrics = {}, Exception {}".format(metrics, e))
        sys.exit(1)

    if len(symbols) == 0 or len(metrics) == 0:
        log.critical("Found zero length for metrics or symbols. Metrics: {}, symbols {}.".format(metrics, symbols))
        sys.exit(1)

    return symbols, metrics


def start_publisher(sqlite, influx, config):
    symbols, metrics = get_symbols_and_metrics(config)
    interval = config.get_value("Settings", "update_interval_seconds")
    log.info("Starting publisher with symbols: {}, metrics: {} and interval: {}".format(symbols, metrics, interval))

    while True:
        for symbol in symbols:
            log.info("Updating symbol {}".format(symbol))
            last_influx_timestamp = CryptoPublisher.get_last_influx_timestamp(influx, symbol, metrics[0])
            new_entries = CryptoPublisher.get_new_sqlite_entries(sqlite, symbol, metrics, last_influx_timestamp)
            log.info("Found {} new entries for symbol {}.".format(len(new_entries), symbol))

            if not influx.insert(symbol, new_entries, metrics, None, True):
                log.error("Failed to add {} new entries to database.".format(len(new_entries)))

        time.sleep(config.get_value("Settings", "update_interval_seconds"))


if __name__ == "__main__":

    working_directory = CryptoPublisher.get_working_directory(PUBLISHER_NAME)
    if working_directory is None:
        sys.exit(1)
    CryptoPublisher.setup_logging(LOG_LEVEL, working_directory, 'log')

    config = setup_configuration(working_directory)

    cmc = setup_cmc(CMC_DIRECTORY)
    if cmc is None:
        sys.exit(1)

    sqlite = setup_sqlite_database(cmc)
    influx = setup_influx_database(config.get_value('Settings', 'influx_db_name'))

    if (sqlite is None) or (influx is None):
        log.critical("Failed to setup required databases")
        sys.exit(1)

    start_publisher(sqlite, influx, config)
