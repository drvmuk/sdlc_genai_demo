"""
Tests for the AURA parser module.
"""
import base64
import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from src.aura_parser import (
    decode_base64, extract_from_json, progressive_decode,
    normalize_xml, apply_xslt_transform, parse_a