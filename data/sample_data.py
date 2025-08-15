"""
Sample data generator for testing the Finance Data Pipeline.
This script creates sample data files that can be used for local testing.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Create data directory if it doesn't exist
os