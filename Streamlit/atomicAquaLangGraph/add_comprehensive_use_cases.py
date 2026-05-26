"""
Add comprehensive use cases for all GNSS troubleshooting questions.
Based on knowledge base validation.
"""

# New use cases to add (validated against knowledge base)
new_use_cases = {
    # Receiver Status & Configuration
    "firmware_version": {
        "keywords": ["firmware version", "firmware", "receiver version", "software version", 
                    "what firmware", "which firmware", "version running", "receiver software"],
        "log_type": "VERSION",
        "field_index": None,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": None,
        "description": "Check firmware/software version from VERSION log",
        "unit": None
    },
    
    "receiver_resets": {
        "keywords": ["receiver reset", "receiver reboot", "power cycle", "power interruption",
                    "unexpected reset", "system reset", "reboot event", "any resets", "reset command"],
        "log_type": "RESET",
        "field_index": None,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": None,
        "description": "Detect receiver resets from RESET or FRESET command logs",
        "unit": None
    },
    
    # Satellite Tracking
    "satellite_count": {
        "keywords": ["how many satellites", "satellite count", "number of satellites",
                    "satellites tracked", "satellites used", "sat count", "tracking satellites"],
        "log_type": "BESTPOS",
        "field_index": 15,  # satellitesTracked
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze number of satellites tracked from BESTPOS",
        "unit": "satellites"
    },
    
    "satellites_used_in_solution": {
        "keywords": ["satellites used in solution", "sats in solution", "satellites for fix",
                    "how many sats used", "satellites in position"],
        "log_type": "BESTPOS",
        "field_index": 16,  # satellitesUsed
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze number of satellites used in position solution from BESTPOS",
        "unit": "satellites"
    },
    
    # Position Solution
    "position_solution_type": {
        "keywords": ["position type", "solution type", "position solution", "what position type",
                    "position mode", "solution mode", "rtk status", "fix type", "gnss position type"],
        "log_type": "BESTPOS",
        "field_index": 3,  # positionType
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": None,
        "description": "Check position solution type from BESTPOS (SINGLE, RTK, PPP, etc.)",
        "unit": None
    },
    
    "solution_status": {
        "keywords": ["solution status", "position status", "sol computed", "solution valid",
                    "position valid", "solution degraded", "insufficient obs"],
        "log_type": "BESTPOS",
        "field_index": 2,  # solutionStatus
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": None,
        "description": "Check solution status from BESTPOS (SOL_COMPUTED, INSUFFICIENT_OBS, etc.)",
        "unit": None
    },
    
    # DOP Analysis
    "pdop_analysis": {
        "keywords": ["pdop", "position dop", "position dilution", "dop value",
                    "dilution of precision", "geometric dop", "high dop"],
        "log_type": "BESTPOS",
        "field_index": 13,  # PDOP (need to verify this is correct field)
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze PDOP (Position Dilution of Precision) from BESTPOS",
        "unit": None
    },
    
    # Position Accuracy
    "horizontal_accuracy": {
        "keywords": ["horizontal accuracy", "horizontal error", "horizontal std dev",
                    "lat lon accuracy", "position accuracy horizontal"],
        "log_type": "BESTPOS",
        "field_index": 9,  # latitudeDeviation (can also check longitudeDeviation)
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze horizontal position accuracy from BESTPOS latitude deviation",
        "unit": "m"
    },
    
    "vertical_accuracy": {
        "keywords": ["vertical accuracy", "height accuracy", "vertical error", "height std dev",
                    "altitude accuracy"],
        "log_type": "BESTPOS",
        "field_index": 11,  # heightDeviation
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze vertical position accuracy from BESTPOS height deviation",
        "unit": "m"
    },
    
    # Differential Corrections
    "differential_age": {
        "keywords": ["differential age", "correction age", "rtk age", "correction latency",
                    "how old corrections", "correction delay"],
        "log_type": "BESTPOS",
        "field_index": 14,  # differentialAge
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "special_handler": None,
        "description": "Analyze age of differential corrections from BESTPOS",
        "unit": "seconds"
    },
    
    # INS/IMU Status
    "ins_solution_status": {
        "keywords": ["ins status", "ins solution", "ins alignment", "ins good",
                    "ins solution good", "ins alignment complete", "ins converged", "ins quality"],
        "log_type": "INSPVA",
        "field_index": None,  # Will use LLM to interpret status field
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": None,
        "description": "Check INS solution status from INSPVA (uses LLM to interpret status field)",
        "unit": None
    },
    
    # Scintillation
    "scintillation_detection": {
        "keywords": ["scintillation", "ionospheric scintillation", "signal scintillation",
                    "detect scintillation", "check scintillation", "is there scintillation",
                    "any scintillation", "scintillation present", "guess scintillation",
                    "can you guess scintillation"],
        "log_type": "TRACKSTAT",
        "field_index": 10,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "special_handler": "trackstat_cno_analysis",
        "description": "Detect ionospheric scintillation from C/No variations in TRACKSTAT",
        "unit": "dB-Hz"
    },
}

print(f"Generated {len(new_use_cases)} comprehensive use cases")
print("\nUse cases:")
for name, config in new_use_cases.items():
    print(f"  - {name}: {config['description']}")
