import logging
import logging.config
import os

LOG_DIR = os.path.expanduser('~') + '/log/batching/'
DEBUG = True
if DEBUG:
    MONGO_DB_SETTINGS = {'host': 'localhost', 'port': 27000}
    BUFFER_TIME = 10
    LOG_DIR = 'log'
else:
    MONGO_DB_SETTINGS = {'host': 'replset/replset-mongo'}
    BUFFER_TIME = 10000


def get_logging_dict(log_dir):
    return {
        'version': 1,
        'disable_existing_loggers': True,
        'filters': {
            'require_debug_false': {
                '()': 'settings.RequireDebugFalse'
            },
            'require_debug_true': {
                '()': 'settings.RequireDebugTrue'
            }
        },
        'formatters': {
            'main_formatter': {
                'format': '%(asctime)s %(filename)s: %(lineno)d %(name)s %(levelname)s: %(message)s ',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'filters': ['require_debug_true'],
                'class': 'logging.StreamHandler',
                'formatter': 'main_formatter',
            },
            'batcher': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(log_dir, 'batcher_debug.txt'),
                'maxBytes': 1024 * 1024 * 5,  # 5 MB
                'backupCount': 7,
                'formatter': 'main_formatter',
                'filters': ['require_debug_true'],
            },
            'batch_info': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(log_dir, 'batcher.txt'),
                'maxBytes': 1024 * 1024 * 5,  # 5 MB
                'backupCount': 7,
                'formatter': 'main_formatter',
                'filters': ['require_debug_false'],
            },
            'looper': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(log_dir, 'looper_debug.txt'),
                'maxBytes': 1024 * 1024 * 5,  # 5 MB
                'backupCount': 7,
                'formatter': 'main_formatter',
                'filters': ['require_debug_true'],
            },
            'looper_info': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(log_dir, 'looper.txt'),
                'maxBytes': 1024 * 1024 * 5,  # 5 MB
                'backupCount': 7,
                'formatter': 'main_formatter',
                'filters': ['require_debug_false'],
            },
        },
        'loggers': {
            'batcher': {
                'handlers': ['console', 'batcher', 'batch_info'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'looper': {
                'handlers': ['console', 'looper', 'looper_info'],
                'level': 'DEBUG',
                'propagate': False,
            }
        }
    }


class RequireDebugFalse(logging.Filter):

    def filter(self, record):
        return not DEBUG


class RequireDebugTrue(logging.Filter):

    def filter(self, record):
        return DEBUG


def init_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    logging.config.dictConfig(get_logging_dict(LOG_DIR))
