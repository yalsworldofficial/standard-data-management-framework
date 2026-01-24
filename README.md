# Standard Data Management Framework

A modular, scalable, and Python-based Data Management Framework designed to standardize ingestion, validation, transformation, metadata handling, and storage across enterprise data workflows.
This framework eliminates repetitive boilerplate and provides a consistent structure for building reliable, maintainable data pipelines.




✅ Correct ways to fix this
Option 1 (Best practice): install your package in editable mode

From the project root (where pyproject.toml is):

pip install -e .


Then run:

python -m sdmf.cli.main
pip install pyspark==3.5.1 delta-spark==3.1.0


✔ This is the recommended solution for a src/ layout.