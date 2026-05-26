"""
Generate comprehensive use cases configuration from NovAtel log structure definitions.
This script extracts field information from logparsers.js and creates deterministic mappings.
"""

import yaml
import re

# Extracted from logparsers.js and NovatelIdConstants.js
LOG_STRUCTURES = {
    "BESTPOS": {
        "message_id": 42,
        "fields": {
            0: {"name": "solutionStatus", "type": "enum", "description": "Solution status"},
            4: {"name": "positionType", "type": "enum", "description": "Position type"},
            8: {"name": "latitude", "type": "double", "description": "Latitude", "unit": "degrees"},
            16: {"name": "longitude", "type": "double", "description": "Longitude", "unit": "degrees"},
            24: {"name": "height", "type": "double", "description": "Height above mean sea level", "unit": "m"},
            32: {"name": "undulation", "type": "float", "description": "Geoidal separation", "unit": "m"},
            36: {"name": "datumId", "type": "enum", "description": "Datum ID"},
            40: {"name": "latitudeDeviation", "type": "float", "description": "Latitude standard deviation", "unit": "m"},
            44: {"name": "longitudeDeviation", "type": "float", "description": "Longitude standard deviation", "unit": "m"},
            48: {"name": "heightDeviation", "type": "float", "description": "Height standard deviation", "unit": "m"},
            52: {"name": "stationId", "type": "string", "description": "Base station ID"},
            56: {"name": "differentialAge", "type": "float", "description": "Differential age", "unit": "s"},
            60: {"name": "solutionAge", "type": "float", "description": "Solution age", "unit": "s"},
            64: {"name": "satellitesTracked", "type": "uchar", "description": "Number of satellites tracked"},
            65: {"name": "satellitesUsed", "type": "uchar", "description": "Number of satellites used in solution"},
            66: {"name": "satellitesUsedInL1", "type": "uchar", "description": "Number of satellites with L1/E1/B1 signals used"},
            67: {"name": "satellitesWithMultiFreqCount", "type": "uchar", "description": "Number of satellites with L1/E1/B1 and L2/E5b/B2 signals used"},
            69: {"name": "extendedSolutionStatus", "type": "hex", "description": "Extended solution status"},
            70: {"name": "signalMaskGalBds", "type": "hex", "description": "Galileo and BeiDou signal mask"},
            71: {"name": "signalMaskGpsGlo", "type": "hex", "description": "GPS and GLONASS signal mask"},
        },
        # Field index mapping (1-based as per NovAtel docs)
        "field_map": {
            1: "header",
            2: "solutionStatus",
            3: "positionType",
            4: "latitude",
            5: "longitude",
            6: "height",
            7: "undulation",
            8: "datumId",
            9: "latitudeDeviation",
            10: "longitudeDeviation",
            11: "heightDeviation",
            12: "stationId",
            13: "differentialAge",
            14: "solutionAge",
            15: "satellitesTracked",
            16: "satellitesUsed",
            17: "satellitesUsedInL1",
            18: "satellitesWithMultiFreqCount",
        }
    },
    "BESTVEL": {
        "message_id": 99,
        "fields": {
            16: {"name": "horSpeed", "type": "double", "description": "Horizontal speed", "unit": "m/s"},
            24: {"name": "trkGround", "type": "double", "description": "Track over ground", "unit": "degrees"},
        },
        "field_map": {
            1: "header",
            2: "solutionStatus",
            3: "velocityType",
            4: "latency",
            5: "age",
            6: "horSpeed",
            7: "trkGround",
            8: "vertSpeed",
        }
    },
    "TRACKSTAT": {
        "message_id": 83,
        "fields": {
            0: {"name": "solutionStatus", "type": "enum", "description": "Solution status"},
            4: {"name": "positionType", "type": "enum", "description": "Position type"},
            8: {"name": "cutoff", "type": "float", "description": "Elevation cutoff angle", "unit": "degrees"},
            12: {"name": "numberOfChannels", "type": "ulong32", "description": "Number of channels"},
            # Per-channel fields (repeating)
            16: {"name": "PRN", "type": "short", "description": "Satellite PRN"},
            18: {"name": "glonassFrequency", "type": "short", "description": "GLONASS frequency"},
            20: {"name": "channelTrackingStatus", "type": "ulong32", "description": "Channel tracking status"},
            24: {"name": "pseudoRange", "type": "double", "description": "Pseudorange", "unit": "m"},
            32: {"name": "doppler", "type": "float", "description": "Doppler frequency", "unit": "Hz"},
            36: {"name": "carrierNoiseRatio", "type": "float", "description": "Carrier-to-noise density ratio", "unit": "dB-Hz"},
            40: {"name": "lockTime", "type": "float", "description": "Lock time", "unit": "s"},
            44: {"name": "pseudoRangeResidual", "type": "float", "description": "Pseudorange residual", "unit": "m"},
            48: {"name": "reject", "type": "enum", "description": "Rejection reason"},
            52: {"name": "pseudoRangeWeight", "type": "float", "description": "Pseudorange weight"},
        },
        "field_map": {
            1: "header",
            2: "solutionStatus",
            3: "positionType",
            4: "cutoff",
            5: "numberOfChannels",
            # Per-channel fields start at field 6
            6: "PRN",
            7: "glonassFrequency",
            8: "channelTrackingStatus",
            9: "pseudoRange",
            10: "doppler",
            11: "carrierNoiseRatio",  # C/No - THIS IS THE KEY FIELD
            12: "lockTime",
            13: "pseudoRangeResidual",
            14: "reject",
            15: "pseudoRangeWeight",
        }
    },
    "RXSTATUS": {
        "message_id": 93,
        "fields": {
            0: {"name": "error", "type": "ulong32", "description": "Error status word"},
            4: {"name": "numStats", "type": "ulong32", "description": "Number of status codes"},
            8: {"name": "rxstat", "type": "ulong32", "description": "Receiver status word"},
            12: {"name": "rxstatPri", "type": "ulong32", "description": "Receiver status priority mask"},
            16: {"name": "rxstatSet", "type": "ulong32", "description": "Receiver status set mask"},
            20: {"name": "rxstatClear", "type": "ulong32", "description": "Receiver status clear mask"},
            24: {"name": "aux1Stat", "type": "ulong32", "description": "Auxiliary 1 status"},
            28: {"name": "aux1StatPri", "type": "ulong32", "description": "Auxiliary 1 status priority"},
            32: {"name": "aux1StatSet", "type": "ulong32", "description": "Auxiliary 1 status set"},
            36: {"name": "aux1StatClear", "type": "ulong32", "description": "Auxiliary 1 status clear"},
            40: {"name": "aux2Stat", "type": "ulong32", "description": "Auxiliary 2 status"},
            44: {"name": "aux2StatPri", "type": "ulong32", "description": "Auxiliary 2 status priority"},
            48: {"name": "aux2StatSet", "type": "ulong32", "description": "Auxiliary 2 status set"},
            52: {"name": "aux2StatClear", "type": "ulong32", "description": "Auxiliary 2 status clear"},
            56: {"name": "aux3Stat", "type": "ulong32", "description": "Auxiliary 3 status"},
            60: {"name": "aux3StatPri", "type": "ulong32", "description": "Auxiliary 3 status priority"},
            64: {"name": "aux3StatSet", "type": "ulong32", "description": "Auxiliary 3 status set"},
            68: {"name": "aux3StatClear", "type": "ulong32", "description": "Auxiliary 3 status clear"},
        },
        "field_map": {
            1: "header",
            2: "error",
            3: "numStats",
            4: "rxstat",  # Main receiver status word - contains jamming (bit 9), spoofing (bit 10), antenna (bit 5)
            5: "rxstatPri",
            6: "rxstatSet",
            7: "rxstatClear",
            8: "aux1Stat",
            9: "aux1StatPri",
            10: "aux1StatSet",
            11: "aux1StatClear",
            12: "aux2Stat",
            13: "aux2StatPri",
            14: "aux2StatSet",
            15: "aux2StatClear",
            16: "aux3Stat",
            17: "aux3StatPri",
            18: "aux3StatSet",
            19: "aux3StatClear",
        },
        "status_bits": {
            0: "Error flag",
            1: "Temperature warning",
            2: "Voltage supply warning",
            3: "Antenna not powered",
            4: "LNA failure",
            5: "Antenna open",
            6: "Antenna shorted",
            7: "CPU overload",
            8: "COM1 buffer overrun",
            9: "Jamming detected",  # KEY BIT
            10: "Spoofing detected",  # KEY BIT
            11: "Antenna not powered",
            12: "Clock model invalid",
        }
    },
    "ITDETECTSTATUS": {
        "message_id": 2065,
        "fields": {
            0: {"name": "noOfEntries", "type": "ulong32", "description": "Number of interference entries"},
            # Per-entry fields (repeating)
            4: {"name": "rfPath", "type": "enum", "description": "RF path"},
            8: {"name": "detectionType", "type": "enum", "description": "Detection type"},
            12: {"name": "centerFrequency", "type": "float", "description": "Center frequency", "unit": "MHz"},
            16: {"name": "bandwidth", "type": "float", "description": "Bandwidth", "unit": "MHz"},
            20: {"name": "power", "type": "float", "description": "Power", "unit": "dBm"},
            24: {"name": "estimatedPower", "type": "float", "description": "Estimated power", "unit": "dBm"},
        },
        "field_map": {
            1: "header",
            2: "noOfEntries",
            # Per-entry fields
            3: "rfPath",
            4: "detectionType",
            5: "centerFrequency",
            6: "bandwidth",
            7: "power",
            8: "estimatedPower",
        }
    },
    "INSPVA": {
        "message_id": 507,
        "fields": {
            0: {"name": "week", "type": "ulong32", "description": "GPS week"},
            4: {"name": "seconds", "type": "double", "description": "Seconds into week"},
            8: {"name": "latitude", "type": "double", "description": "Latitude", "unit": "degrees"},
            16: {"name": "longitude", "type": "double", "description": "Longitude", "unit": "degrees"},
            24: {"name": "height", "type": "double", "description": "Height", "unit": "m"},
            36: {"name": "northVelocity", "type": "double", "description": "North velocity", "unit": "m/s"},
            44: {"name": "eastVelocity", "type": "double", "description": "East velocity", "unit": "m/s"},
            52: {"name": "upVelocity", "type": "double", "description": "Up velocity", "unit": "m/s"},
            60: {"name": "roll", "type": "double", "description": "Roll", "unit": "degrees"},
            68: {"name": "pitch", "type": "double", "description": "Pitch", "unit": "degrees"},
            76: {"name": "azimuth", "type": "double", "description": "Azimuth", "unit": "degrees"},
            84: {"name": "status", "type": "enum", "description": "INS status"},
        },
        "field_map": {
            1: "header",
            2: "week",
            3: "seconds",
            4: "latitude",
            5: "longitude",
            6: "height",
            7: "northVelocity",
            8: "eastVelocity",
            9: "upVelocity",
            10: "roll",
            11: "pitch",
            12: "azimuth",
            13: "status",
        }
    },
    "HEADING2": {
        "message_id": 1335,
        "fields": {
            0: {"name": "solStatus", "type": "enum", "description": "Solution status"},
            4: {"name": "posType", "type": "enum", "description": "Position type"},
            8: {"name": "length", "type": "float", "description": "Baseline length", "unit": "m"},
            12: {"name": "heading", "type": "float", "description": "Heading", "unit": "degrees"},
            16: {"name": "pitch", "type": "float", "description": "Pitch", "unit": "degrees"},
            24: {"name": "hdgStdDev", "type": "float", "description": "Heading standard deviation", "unit": "degrees"},
            28: {"name": "ptchStdDev", "type": "float", "description": "Pitch standard deviation", "unit": "degrees"},
        },
        "field_map": {
            1: "header",
            2: "solStatus",
            3: "posType",
            4: "length",
            5: "heading",
            6: "pitch",
            7: "reserved",
            8: "hdgStdDev",
            9: "ptchStdDev",
        }
    },
}

def generate_enhanced_use_cases():
    """Generate comprehensive use cases from log structures."""
    
    use_cases = {}
    
    # BESTPOS use cases
    use_cases["height_analysis"] = {
        "keywords": ["height", "altitude", "elevation", "vertical", "height field", "altitude field",
                    "minimum height", "maximum height", "average height", "min height", "max height",
                    "lowest height", "highest height", "height above sea level", "msl"],
        "log_type": "BESTPOS",
        "field_index": 6,  # height field (1-based: header=1, solutionStatus=2, positionType=3, lat=4, lon=5, height=6)
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze height/altitude statistics from BESTPOS",
        "unit": "m"
    }
    
    use_cases["latitude_analysis"] = {
        "keywords": ["latitude", "lat", "north", "south", "latitude field",
                    "minimum latitude", "maximum latitude", "average latitude", "northing"],
        "log_type": "BESTPOS",
        "field_index": 4,  # latitude field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze latitude statistics from BESTPOS",
        "unit": "degrees"
    }
    
    use_cases["longitude_analysis"] = {
        "keywords": ["longitude", "lon", "long", "east", "west", "longitude field",
                    "minimum longitude", "maximum longitude", "average longitude", "easting"],
        "log_type": "BESTPOS",
        "field_index": 5,  # longitude field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze longitude statistics from BESTPOS",
        "unit": "degrees"
    }
    
    use_cases["horizontal_accuracy"] = {
        "keywords": ["horizontal accuracy", "horizontal error", "horizontal precision",
                    "position accuracy", "position error", "position precision",
                    "latitude deviation", "longitude deviation", "horizontal stddev"],
        "log_type": "BESTPOS",
        "field_index": 9,  # latitudeDeviation field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze horizontal position accuracy from BESTPOS",
        "unit": "m"
    }
    
    use_cases["vertical_accuracy"] = {
        "keywords": ["vertical accuracy", "vertical error", "vertical precision",
                    "height accuracy", "height error", "altitude accuracy",
                    "height deviation", "height stddev"],
        "log_type": "BESTPOS",
        "field_index": 11,  # heightDeviation field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze vertical position accuracy from BESTPOS",
        "unit": "m"
    }
    
    use_cases["satellites_tracked"] = {
        "keywords": ["satellites tracked", "satellite count", "number of satellites",
                    "sat count", "tracking satellites", "minimum satellites",
                    "maximum satellites", "average satellites", "how many satellites",
                    "satellite number", "sats tracked"],
        "log_type": "BESTPOS",
        "field_index": 15,  # satellitesTracked field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze number of satellites tracked from BESTPOS",
        "unit": "satellites"
    }
    
    use_cases["satellites_used"] = {
        "keywords": ["satellites used", "satellites in solution", "sats used",
                    "satellites used in solution", "number of satellites used"],
        "log_type": "BESTPOS",
        "field_index": 16,  # satellitesUsed field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze number of satellites used in solution from BESTPOS",
        "unit": "satellites"
    }
    
    # BESTVEL use cases
    use_cases["velocity_analysis"] = {
        "keywords": ["velocity", "speed", "vel", "horizontal speed", "ground speed",
                    "minimum velocity", "maximum velocity", "average velocity",
                    "min speed", "max speed", "fastest", "slowest", "how fast"],
        "log_type": "BESTVEL",
        "field_index": 6,  # horSpeed field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze velocity/speed statistics from BESTVEL",
        "unit": "m/s"
    }
    
    use_cases["track_over_ground"] = {
        "keywords": ["track over ground", "track", "heading", "direction",
                    "course", "bearing", "track angle"],
        "log_type": "BESTVEL",
        "field_index": 7,  # trkGround field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze track over ground from BESTVEL",
        "unit": "degrees"
    }
    
    # TRACKSTAT use cases
    use_cases["signal_quality"] = {
        "keywords": ["signal quality", "signal strength", "c/no", "cno", "carrier",
                    "signal level", "carrier to noise", "minimum signal",
                    "maximum signal", "average signal", "signal statistics",
                    "carrier noise ratio", "snr"],
        "log_type": "TRACKSTAT",
        "field_index": 11,  # carrierNoiseRatio field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze signal quality (C/No) from TRACKSTAT",
        "unit": "dB-Hz"
    }
    
    use_cases["scintillation_analysis"] = {
        "keywords": ["scintillation", "scintillating", "ionospheric",
                    "signal fade", "signal variation", "signal fluctuation",
                    "signal fading", "ionospheric scintillation", "signal drops"],
        "log_type": "TRACKSTAT",
        "field_index": 11,  # carrierNoiseRatio field - scintillation shows as rapid variations
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Detect scintillation from C/No variations in TRACKSTAT",
        "unit": "dB-Hz",
        "special_handler": "scintillation"
    }
    
    # RXSTATUS use cases (bit checks)
    use_cases["jamming_detection"] = {
        "keywords": ["jamming", "jammer", "jam", "jammed", "jamming detected",
                    "jamming detection", "any jamming", "check jamming",
                    "detect jamming", "jammer present", "jamming event"],
        "log_type": "RXSTATUS",
        "field_index": 4,  # rxstat field
        "bit_position": 9,  # Jamming detected bit
        "analysis_type": "bit_check",
        "description": "Detect jamming events from RXSTATUS receiver status word",
        "unit": None
    }
    
    use_cases["spoofing_detection"] = {
        "keywords": ["spoofing", "spoof", "spoofed", "fake signal",
                    "spoofing detected", "spoofing detection", "any spoofing",
                    "check spoofing", "detect spoofing", "spoofer present",
                    "spoofing event", "spoofing attack"],
        "log_type": "RXSTATUS",
        "field_index": 4,  # rxstat field
        "bit_position": 10,  # Spoofing detected bit
        "analysis_type": "bit_check",
        "description": "Detect spoofing events from RXSTATUS receiver status word",
        "unit": None
    }
    
    use_cases["antenna_error"] = {
        "keywords": ["antenna", "antenna error", "antenna issue", "antenna problem",
                    "antenna fault", "antenna status", "antenna open", "antenna short",
                    "antenna disconnected", "antenna failure"],
        "log_type": "RXSTATUS",
        "field_index": 4,  # rxstat field
        "bit_position": 5,  # Antenna error bit
        "analysis_type": "bit_check",
        "description": "Detect antenna errors from RXSTATUS",
        "unit": None
    }
    
    # ITDETECTSTATUS use cases
    use_cases["interference_detection"] = {
        "keywords": ["interference", "interfere", "rf interference", "spectrum",
                    "interference detected", "interference detection",
                    "any interference", "check interference", "detect interference",
                    "rf jamming", "narrowband interference", "wideband interference"],
        "log_type": "ITDETECTSTATUS",
        "field_index": 2,  # noOfEntries field (if > 0, interference detected)
        "bit_position": None,
        "analysis_type": "raw_listing",  # Need to show details of interference
        "description": "Detect RF interference from ITDETECTSTATUS log",
        "unit": None
    }
    
    # INS use cases
    use_cases["roll_analysis"] = {
        "keywords": ["roll", "roll angle", "bank", "bank angle",
                    "minimum roll", "maximum roll", "average roll"],
        "log_type": "INSPVA",
        "field_index": 10,  # roll field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze roll angle from INSPVA",
        "unit": "degrees"
    }
    
    use_cases["pitch_analysis"] = {
        "keywords": ["pitch", "pitch angle", "nose up", "nose down",
                    "minimum pitch", "maximum pitch", "average pitch"],
        "log_type": "INSPVA",
        "field_index": 11,  # pitch field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze pitch angle from INSPVA",
        "unit": "degrees"
    }
    
    use_cases["azimuth_analysis"] = {
        "keywords": ["azimuth", "heading", "yaw", "direction",
                    "minimum azimuth", "maximum azimuth", "average azimuth"],
        "log_type": "INSPVA",
        "field_index": 12,  # azimuth field
        "bit_position": None,
        "analysis_type": "numeric_stat",
        "description": "Analyze azimuth/heading from INSPVA",
        "unit": "degrees"
    }
    
    # Direct handlers (no LLM needed)
    use_cases["list_logs"] = {
        "keywords": ["list all logs", "list logs", "what logs", "all logs",
                    "log types", "how many logs", "available logs", "logs in file",
                    "logs present", "show logs", "display logs", "log list"],
        "log_type": None,
        "field_index": None,
        "bit_position": None,
        "analysis_type": "direct_handler",
        "handler_name": "list_logs",
        "description": "List all log types in the file",
        "unit": None
    }
    
    use_cases["time_range"] = {
        "keywords": ["time range", "start time", "end time", "duration",
                    "file time", "gps time", "utc time", "how long",
                    "start and end", "time span", "time period", "file duration"],
        "log_type": None,
        "field_index": None,
        "bit_position": None,
        "analysis_type": "direct_handler",
        "handler_name": "time_range",
        "description": "Show file time range",
        "unit": None
    }
    
    use_cases["data_gaps"] = {
        "keywords": ["data gap", "data gaps", "missing data", "gaps",
                    "continuous", "continuity", "data loss", "time gap",
                    "time gaps", "missing time", "dropped data"],
        "log_type": None,
        "field_index": None,
        "bit_position": None,
        "analysis_type": "direct_handler",
        "handler_name": "data_gaps",
        "description": "Check for time gaps in data",
        "unit": None
    }
    
    # Complex/listing use cases (requires LLM interpretation)
    use_cases["receiver_status_summary"] = {
        "keywords": ["receiver status", "rx status", "receiver health",
                    "system status", "receiver summary", "status summary"],
        "log_type": "RXSTATUS",
        "field_index": None,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "description": "Summarize receiver status records",
        "unit": None
    }
    
    use_cases["tracking_status_summary"] = {
        "keywords": ["tracking status", "tracking", "satellite tracking",
                    "track", "tracking summary", "channel status"],
        "log_type": "TRACKSTAT",
        "field_index": None,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "description": "Summarize satellite tracking status",
        "unit": None
    }
    
    use_cases["position_summary"] = {
        "keywords": ["position", "coordinates", "location", "pos",
                    "position summary", "coordinate summary"],
        "log_type": "BESTPOS",
        "field_index": None,
        "bit_position": None,
        "analysis_type": "raw_listing",
        "description": "Summarize position records",
        "unit": None
    }
    
    # Fallback mappings
    fallback_mappings = {
        "detection": ["RXSTATUS", "ITDETECTSTATUS", "RXSTATUSEVENT"],
        "position": ["BESTPOS", "BESTGNSSPOS", "PSRPOS"],
        "velocity": ["BESTVEL", "BESTGNSSVEL", "PSRVEL"],
        "signal": ["TRACKSTAT", "SATVIS", "RANGE"],
        "time": ["CLOCKSTEERING", "TIMESYNC", "TIME"],
        "ins": ["INSPVA", "INSPVAX", "INSPOSX"],
        "heading": ["HEADING2", "HEADING", "DUALANTENNAHEADING"],
    }
    
    return {"use_cases": use_cases, "fallback_mappings": fallback_mappings}

if __name__ == "__main__":
    config = generate_enhanced_use_cases()
    
    # Write to YAML file
    with open("src/use_cases_config_enhanced.yaml", "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    print(f"✅ Generated enhanced use cases configuration with {len(config['use_cases'])} use cases")
    print(f"✅ Total keywords: {sum(len(uc.get('keywords', [])) for uc in config['use_cases'].values())}")
    print(f"✅ Saved to: src/use_cases_config_enhanced.yaml")
    
    # Print summary
    print("\n📊 Use Cases Summary:")
    for name, uc in config['use_cases'].items():
        print(f"  - {name}: {uc['log_type']} field {uc.get('field_index', 'N/A')} "
              f"({uc['analysis_type']}) - {len(uc.get('keywords', []))} keywords")
