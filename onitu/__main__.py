#!/usr/bin/env python

import argparse
import json
from importlib import import_module

from logbook import Logger, INFO, DEBUG, NullHandler, NestedSetup
from logbook.queues import ZeroMQHandler, ZeroMQSubscriber
from logbook.more import ColorizedStderrHandler

from .cutils import get_open_port

logger = None

def get_logs_dispatcher(uri=None, debug=False):
    """Configure the dispatcher that will print the logs received
    on the ZeroMQ channel.
    """
    handlers = []

    if not debug:
        handlers.append(NullHandler(level=DEBUG))

    handlers.append(ColorizedStderrHandler(level=INFO))

    if not uri:
        uri = get_open_port()

    subscriber = ZeroMQSubscriber(uri, multi=True)
    return uri, subscriber.dispatch_in_background(setup=NestedSetup(handlers))


def get_setup(setup_file):
    if setup_file.endswith(('.yml', '.yaml')):
        try:
            import yaml
        except ImportError:
            logger.error(
                "You provided a YAML setup file, but PyYAML was not found on "
                "your system."
            )
        loader = lambda f: yaml.load(f.read())
    elif setup_file.endswith('.json'):
        import json
        loader = json.load
    else:
        logger.error(
            "The setup file must be either in JSON or YAML."
        )
        return

    try:
        with open(setup_file) as f:
            return loader(f)
    except ValueError as e:
        logger.error("Error parsing '{}' : {}", setup_file, e)
    except Exception as e:
        logger.error(
            "Can't process setup file '{}' : {}", setup_file, e
        )

def main():
    global logger

    logger = Logger("Onitu client")

    parser = argparse.ArgumentParser("onitu")
    parser.add_argument(
        '--setup', default='setup.yml',
        help="A JSON file with Onitu's configuration (defaults to setup.json)"
    )
    parser.add_argument(
        '--log-uri', help="A ZMQ socket where all the logs will be sent"
    )
    parser.add_argument(
        '--debug', action='store_true', help="Enable debugging logging"
    )
    args = parser.parse_args()

    log_uri, dispatcher = get_logs_dispatcher(
        uri=args.log_uri, debug=args.debug
    )

    with ZeroMQHandler(log_uri, multi=True):
        setup = get_setup(args.setup)
        if setup is None:
            return
        driver = None

        try:
            driver = import_module("onitu.drivers.{}".format(setup['service']['driver']))
            driver.plug.initialize(setup)
            driver.start()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            if driver is not None:
                driver.plug.close()
            logger.info("Exiting...")
            if dispatcher:
                dispatcher.stop()

if __name__ == '__main__':
    main()
