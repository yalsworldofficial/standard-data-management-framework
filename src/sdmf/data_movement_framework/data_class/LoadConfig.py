# inbuilt
import configparser
from dataclasses import dataclass


@dataclass
class LoadConfig:
    load_specs: dict
    target_unity_catalog: str
    target_schema_name: str
    target_table_name: str
    config: configparser.ConfigParser
