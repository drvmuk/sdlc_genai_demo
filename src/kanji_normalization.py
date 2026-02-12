"""
Kanji normalization module for E2E Policy Services BC K2H Data Extraction.

This module implements the Kanji character normalization logic as specified in the
business requirements. It validates Kanji characters against acceptable ranges
and replaces invalid characters with a solid black square (Unicode U+25A0).
"""

def normalize_kanji_name(kanji_name: str) -> str:
    """
    Normalize Kanji names by validating characters against acceptable ranges
    and replacing invalid characters with a solid black square (Unicode U+25A0).
    
    This function implements the logic from the Java Custom transformations
    JTX_LAST_NAME_KANJI_CONVERSION and JTX_FIRST_NAME_KANJI_CONVERSION.
    
    Args:
        kanji_name: Input Kanji name string
        
    Returns:
        str: Normalized Kanji name with invalid characters replaced
    """
    if kanji_name is None:
        return ""
    
    # Replace character function
    def is_valid_kanji_char(char):
        # Convert to code point
        code_point = ord(char)
        
        # ASCII subset (allowed)
        if 0 <= code_point <= 127:
            return True
        
        # Handle Shift-JIS double-byte ranges
        # These are the allowed ranges from the requirements:
        # 20-62, 64-223, 33088-33124, 33129-33184, 33186-33686, 34975-39026, 39071-60066
        if (20 <= code_point <= 62 or
            64 <= code_point <= 223 or
            33088 <= code_point <= 33124 or
            33129 <= code_point <= 33184 or
            33186 <= code_point <= 33686 or
            34975 <= code_point <= 39026 or
            39071 <= code_point <= 60066):
            return True
        
        return False
    
    # Process the input string
    result = []
    for char in kanji_name:
        # Handle newline characters specially
        if char == '\r':
            # Remove carriage returns
            continue
        elif char == '\n':
            # Keep line feeds
            result.append('\n')
        elif is_valid_kanji_char(char):
            # Keep valid characters
            result.append(char)
        else:
            # Replace invalid characters with black square
            result.append('\u25A0')  # Unicode U+25A0 (BLACK SQUARE)
    
    return ''.join(result)