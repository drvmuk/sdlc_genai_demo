"""
Module for parsing and transforming AURA payloads.
"""
import base64
import json
import logging
import re
from typing import Optional, Tuple

from lxml import etree
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def decode_base64(encoded_str: str) -> str:
    """
    Decode a base64 encoded string.
    
    Args:
        encoded_str: Base64 encoded string
        
    Returns:
        Decoded string
    """
    if not encoded_str:
        return ""
    
    try:
        return base64.b64decode(encoded_str).decode('utf-8')
    except Exception as e:
        logger.error(f"Error decoding base64: {e}")
        return ""


def extract_from_json(json_str: str) -> str:
    """
    Extract interviewDetails field from JSON string.
    
    Args:
        json_str: JSON string potentially containing interviewDetails
        
    Returns:
        Extracted interviewDetails or original string if not JSON or no interviewDetails
    """
    if not json_str or not json_str.startswith("{"):
        return json_str
    
    try:
        json_obj = json.loads(json_str)
        if "interviewDetails" in json_obj:
            return json_obj["interviewDetails"]
        return json_str
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
        return json_str


def progressive_decode(encoded_str: str, max_iterations: int = 100) -> str:
    """
    Progressively decode a potentially multi-layered base64 encoded string.
    
    Args:
        encoded_str: Potentially multi-layered base64 encoded string
        max_iterations: Maximum number of decode iterations
        
    Returns:
        Decoded string
    """
    if not encoded_str:
        return ""
    
    result = encoded_str
    iterations = 0
    
    while iterations < max_iterations and not result.startswith("<"):
        try:
            decoded = decode_base64(result)
            if not decoded or decoded == result:
                break
            result = decoded
            iterations += 1
        except Exception as e:
            logger.error(f"Error in progressive decode iteration {iterations}: {e}")
            break
    
    return result


def normalize_xml(xml_str: str) -> str:
    """
    Normalize XML by removing encoding declaration and wrapping in XMLA container.
    
    Args:
        xml_str: XML string to normalize
        
    Returns:
        Normalized XML string
    """
    if not xml_str or not xml_str.startswith("<"):
        return xml_str
    
    # Remove XML declaration encoding attribute
    normalized = re.sub(r'(<\?.*)encoding(.*\?>)', r'\1\2', xml_str)
    
    # Wrap in XMLA container
    return f"<XMLA>{normalized}</XMLA>"


def apply_xslt_transform(xml_str: str, xslt_path: str) -> str:
    """
    Apply XSLT transformation to XML string.
    
    Args:
        xml_str: XML string to transform
        xslt_path: Path to XSLT file
        
    Returns:
        Transformed XML string
    """
    if not xml_str or not xml_str.startswith("<"):
        return ""
    
    try:
        # Parse XML and XSLT
        xml_doc = etree.fromstring(xml_str.encode('utf-8'))
        xslt_doc = etree.parse(xslt_path)
        transform = etree.XSLT(xslt_doc)
        
        # Apply transformation
        result = transform(xml_doc)
        return str(result)
    except Exception as e:
        logger.error(f"Error applying XSLT transform: {e}")
        return ""


def parse_aura_payload(
    aura_input: str, 
    policy_number: str,
    xslt_file: str,
    aura15_xslt_file: str
) -> Tuple[Optional[str], str]:
    """
    Parse and transform AURA payload.
    
    Args:
        aura_input: Base64 encoded AURA payload
        policy_number: Associated policy number
        xslt_file: Path to XSLT file for Aura 14
        aura15_xslt_file: Path to XSLT file for Aura 15
        
    Returns:
        Tuple of (parsed AURA output, policy number)
    """
    if not aura_input:
        logger.info("AURA input is null or empty")
        return None, policy_number
    
    try:
        logger.info("Before AURA transformer")
        
        # Initial decode
        aura_output = decode_base64(aura_input)
        
        # Check if JSON and extract interviewDetails if present
        if aura_output.startswith("{"):
            aura_output = extract_from_json(aura_output)
            logger.info("After JSON extract")
        
        # Progressive decoding
        aura_output = progressive_decode(aura_output)
        logger.info("After progressive decode")
        
        # If not XML after decoding, return None
        if not aura_output.startswith("<"):
            logger.info("AURA input was not XML after decoding")
            return None, policy_number
        
        # Select XSLT based on content
        selected_xslt = aura15_xslt_file if "TXLife" in aura_output else xslt_file
        logger.info(f"Selected XSLT: {'Aura15' if 'TXLife' in aura_output else 'Aura14'}")
        
        # Normalize XML
        normalized_xml = normalize_xml(aura_output)
        logger.info("After XML normalization")
        
        # Apply XSLT transformation
        parsed_output = apply_xslt_transform(normalized_xml, selected_xslt)
        logger.info("After AURA transformer for Conversion")
        
        return parsed_output, policy_number
    except Exception as e:
        logger.error(f"Error in AURA parsing: {e}")
        logger.error(f"Current AURA_OUTPUT: {aura_input[:100]}...")  # Log truncated for security
        return None, policy_number


# Define UDF for use in Spark
parse_aura_payload_udf = udf(parse_aura_payload, StringType())