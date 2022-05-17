import logging.config
import os

log_config_dict = \
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s"
            }
        },

        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "WARNING",
                "formatter": "simple",
                "stream": "ext://sys.stdout"
            },

            "info_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "info.log",
                "level": "INFO",
                "formatter": "simple",
                'maxBytes': 100 * 1024 * 1024,
                'backupCount': 10,
            },

            "error_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "error.log",
                "level": "WARNING",
                "formatter": "simple",
                'maxBytes': 100 * 1024 * 1024,
                'backupCount': 10,
            }
        },

        "loggers": {
            "my_module": {
                "level": "ERROR",
                "handlers": ["console"],
                "propagate": "no"
            }
        },

        "root": {
            "level": "DEBUG",
            "handlers": ["console", ]
        }
    }


def setup_logging(info_log_file=None, error_log_file=None, info_log_file_level="INFO"):
    if info_log_file and os.path.exists(os.path.dirname(info_log_file)):
        log_config_dict["root"]["handlers"].append("info_file_handler")
        log_config_dict["handlers"]["info_file_handler"]["filename"] = info_log_file
        log_config_dict["handlers"]["info_file_handler"]["level"] = info_log_file_level
    if error_log_file and os.path.exists(os.path.dirname(error_log_file)):
        log_config_dict["root"]["handlers"].append("error_file_handler")
        log_config_dict["handlers"]["error_file_handler"]["filename"] = error_log_file
    logging.config.dictConfig(log_config_dict)


ss_kubeflow_pipeline_component_logger = logging.getLogger("ss_common_recommend_system_logger")


def setup_console_log(level=logging.INFO):
    logging.basicConfig(format='[%(asctime)s] - %(pathname)s line:%(lineno)d - %(levelname)s: %(message)s',
                        level=level)
