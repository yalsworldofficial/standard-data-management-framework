# inbuilt
import uuid
import logging

# external
import pandas as pd


class SystemRestore():

    def __init__(self, master_specs: list[dict], run_id: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.master_specs = master_specs
        self.run_id = run_id

    def on_demand_restore(self, restore_point_id: str, restore_reason: str):
        pass

    def run(self):
        pass

    def __get_target_state(self):
        for current_spec in self.master_specs:
            feed_id = current_spec['feed_id']
            
