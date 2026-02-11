"""
AURA payload parsing and transformation module.
Implements Java-equivalent functionality for decoding and transforming AURA payloads.
"""
import base64
import json
import re
import logging
from io import StringIO
from lxml import etree

def parse_aura_payload(i_aura_input_base64, policy_number, xslt_file_path, aura15_xslt_file_path):
    """
    Parse and transform AURA payload from base64 to canonical XML.
    
    Args:
        i_aura_input_base64 (str): Base64-encoded AURA payload
        policy_number (str): Policy number associated with the payload
        xslt_file_path (str): Path to XSLT file for Aura 14
        aura15_xslt_file_path (str): Path to XSLT file for Aura 15
        
    Returns:
        tuple: (parsed_aura_output, policy_number)
    """
    if not i_aura_input_base64 or i_aura_input_base64.strip() == '':
        logging.warning("AURA input is null or empty")
        return None, policy_number
    
    logging.info("Before AURA transformer")
    
    # Step 2: Base64/JSON Preprocessing
    try:
        aura_output = base64.b64decode(i_aura_input_base64).decode('utf-8')
        
        # Check if JSON and extract interviewDetails if needed
        if aura_output.startswith('{'):
            try:
                json_data = json.loads(aura_output)
                if 'interviewDetails' in json_data:
                    aura_output = json_data['interviewDetails']
                    logging.info("Extracted interviewDetails from JSON")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON: {str(e)}")
    except Exception as e:
        logging.error(f"Initial base64 decode failed: {str(e)}")
        return None, policy_number
    
    # Step 3: Progressive Decoding Loop
    decode_count = 0
    while not aura_output.startswith('<') and decode_count < 100:
        try:
            decoded = base64.b64decode(aura_output).decode('utf-8')
            aura_output = decoded
            decode_count += 1
            logging.debug(f"Decoded layer {decode_count}")
            
            if aura_output.startswith('<'):
                break
        except Exception as e:
            logging.error(f"Failed at decode iteration {decode_count}: {str(e)}")
            break
    
    logging.info(f"Completed {decode_count} decode iterations")
    
    # Step 4: Canonical XML Detection
    if not aura_output.startswith('<'):
        logging.warning("AURA input was not valid XML after decoding")
        return None, policy_number
    
    # Step 5: XSLT Selection Rule
    if "TXLife" in aura_output:
        xslt_path = aura15_xslt_file_path
        logging.info("Using Aura 15 XSLT")
    else:
        xslt_path = xslt_file_path
        logging.info("Using Aura 14 XSLT")
    
    # Step 6: XML Normalization
    # Remove XML declaration encoding attribute
    aura_output = re.sub(r'(<\?.*)encoding(.*\?>)', r'\1\2', aura_output)
    
    # Wrap in XMLA container
    wrapped_xml = f"<XMLA>{aura_output}</XMLA>"
    
    logging.info("After Replace and Concat")
    
    # Step 7: XSLT Transformation
    try:
        # Load XSLT
        xslt_root = etree.parse(xslt_path)
        transform = etree.XSLT(xslt_root)
        
        # Parse and transform XML
        xml_root = etree.parse(StringIO(wrapped_xml))
        result_tree = transform(xml_root)
        parsed_aura_output = str(result_tree)
        
        logging.info("After AURA transformer for Conversion")
        return parsed_aura_output, policy_number
        
    except Exception as e:
        logging.error(f"XSLT transformation failed: {str(e)}")
        return None, policy_number