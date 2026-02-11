"""
Unit tests for the AURA parser module.
"""
import unittest
import base64
import os
import tempfile
from src.aura_parser import parse_aura_payload

class TestAuraParser(unittest.TestCase):
    
    def setUp(self):
        # Create temporary XSLT files for testing
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a simple XSLT file for Aura 14
        self.xslt_file = os.path.join(self.temp_dir, "aura14.xslt")
        with open(self.xslt_file, "w") as f:
            f.write("""<?xml version="1.0" encoding="UTF-8"?>
            <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                <xsl:template match="/">
                    <TransformedAura14>
                        <xsl:value-of select="/XMLA/Aura/Data"/>
                    </TransformedAura14>
                </xsl:template>
            </xsl:stylesheet>""")
        
        # Create a simple XSLT file for Aura 15
        self.aura15_xslt_file = os.path.join(self.temp_dir, "aura15.xslt")
        with open(self.aura15_xslt_file, "w") as f:
            f.write("""<?xml version="1.0" encoding="UTF-8"?>
            <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                <xsl:template match="/">
                    <TransformedAura15>
                        <xsl:value-of select="/XMLA/Aura/TXLife/Data"/>
                    </TransformedAura15>
                </x