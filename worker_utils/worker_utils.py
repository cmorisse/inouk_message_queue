#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from datetime import datetime


def parse_memory_limit(limit_str):
    """Parse memory limit string (e.g., '1024M', '2G') to bytes
    
    Args:
        limit_str (str): Memory limit string with optional suffix (K, M, G)
        
    Returns:
        int: Memory limit in bytes, or None if invalid
    """
    if not limit_str:
        return None
        
    # Define multipliers
    multipliers = {
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4
    }
    
    # Clean and normalize the input
    limit_str = limit_str.upper().strip()
    
    # Check for suffix
    for suffix, multiplier in multipliers.items():
        if limit_str.endswith(suffix):
            try:
                value = int(limit_str[:-1])
                if value < 0:
                    return None
                return value * multiplier
            except ValueError:
                return None
    
    # No suffix, assume bytes
    try:
        value = int(limit_str)
        return value if value >= 0 else None
    except ValueError:
        return None


def format_duration(seconds):
    """Format duration in seconds to human-readable string
    
    Args:
        seconds (float): Duration in seconds
        
    Returns:
        str: Formatted duration string
    """
    if seconds < 1:
        return f"{seconds:.3f}s"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m{secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h{minutes}m"


def format_memory_size(bytes_size):
    """Format memory size in bytes to human-readable string
    
    Args:
        bytes_size (int): Memory size in bytes
        
    Returns:
        str: Formatted memory size string
    """
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 ** 2:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 ** 3:
        return f"{bytes_size / (1024 ** 2):.1f} MB"
    elif bytes_size < 1024 ** 4:
        return f"{bytes_size / (1024 ** 3):.1f} GB"
    else:
        return f"{bytes_size / (1024 ** 4):.1f} TB"


def validate_queue_pattern(pattern):
    """Validate queue pattern is a valid regex or simple pattern
    
    Args:
        pattern (str): Queue pattern to validate
        
    Returns:
        bool: True if valid pattern, False otherwise
    """
    if not pattern or not isinstance(pattern, str):
        return False
        
    # Allow simple patterns with wildcards
    if '*' in pattern or pattern.isalnum() or '_' in pattern or '-' in pattern:
        return True
    
    # Test as regex
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False


def get_worker_name(custom_name=None):
    """Generate worker name
    
    Args:
        custom_name (str): Custom worker name, or None for auto-generated
        
    Returns:
        str: Worker name
    """
    if custom_name:
        return custom_name
    
    import os
    import socket
    
    hostname = socket.gethostname()
    pid = os.getpid()
    
    # Use hostname and PID for uniqueness without timestamp
    # This makes worker names more predictable and Kubernetes-friendly
    return f"imq-worker-{hostname}-{pid}"


def safe_int(value, default=0):
    """Safely convert value to int
    
    Args:
        value: Value to convert
        default (int): Default value if conversion fails
        
    Returns:
        int: Converted value or default
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default