"""
Utility functions for the E2E Address Change YUYU Creation workflow.
"""
import os
import logging
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

def create_trigger_file(directory: str, filename: str, content: str) -> None:
    """
    Create a trigger file with the specified content.
    
    Args:
        directory: Directory for the trigger file
        filename: Name of the trigger file
        content: Content to write to the trigger file
    """
    try:
        file_path = os.path.join(directory, filename)
        logger.info(f"Creating trigger file: {file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(directory,