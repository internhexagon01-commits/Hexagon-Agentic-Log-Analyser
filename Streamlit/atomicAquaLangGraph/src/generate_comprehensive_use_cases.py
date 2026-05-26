"""
Generate COMPREHENSIVE use cases configuration from ALL NovAtel logs.
Extracted from logparsers.js - includes 50+ log types.
"""

import yaml

# ALL LOG TYPES from logparsers.js with their common keywords
ALL_LOGS = {
    # Position & Navigation Logs
    "BESTPOS": {
        "keywords": ["position", "best position", "coordinates", "location", "lat", "lon", "height", "altitude"],
        "category": "position",
        "description": "Best available GNSS position solution"
    },
    "BESTVEL": {
        "keywords": ["velocity", "speed", "vel", "track", "heading", "direction"],
        "category": "velocity",
        "description": "Best available GNSS velocity solution"
    },
    "AVEPOS": {
        "keywords": ["average position", "averaged position", "position average"],
        "category": "position",
        "description": "Averaged position over time"
    },
    "MARKPOS": {
        "keywords": ["mark position", "marked position", "position mark"],
        "category": "position",
        "description": "Marked position at specific event"
    },
    "PSRPOS": {
        "keywords": ["pseudorange position", "psr position"],
        "category": "position",
        "description": "Pseudorange-based position"
    },
    
    # INS/Attitude Logs
    "INSPVA": {
        "keywords": ["ins", "inertial", "attitude", "roll", "pitch", "azimuth", "yaw"],
        "category": "ins",
        "description": "INS position, velocity, and attitude"
    },
    "INSPVAX": {
        "keywords": ["ins extended", "inspvax", "ins accuracy"],
        "category": "ins",
        "description": "Extended INS solution with accuracies"
    },
    "INSPOSX": {
        "keywords": ["ins position extended", "insposx"],
        "category": "ins",
        "description": "Extended INS position"
    },
    "MARKPVA": {
        "keywords": ["mark pva", "marked ins"],
        "category": "ins",
        "description": "Marked INS position/velocity/attitude"
    },
    
    # Heading Logs
    "HEADING2": {
        "keywords": ["heading", "dual antenna heading", "baseline heading"],
        "category": "heading",
        "description": "Dual-antenna heading solution"
    },
    
    # Signal Quality & Tracking Logs
    "TRACKSTAT": {
        "keywords": ["tracking", "track status", "signal quality", "c/no", "cno", "carrier"],
        "category": "signal",
        "description": "Satellite tracking status and signal quality"
    },
    "SATVIS2": {
        "keywords": ["satellite visibility", "sat vis", "visible satellites"],
        "category": "signal",
        "description": "Satellite visibility and almanac status"
    },
    "BESTSATS": {
        "keywords": ["best satellites", "satellite selection"],
        "category": "signal",
        "description": "Best satellites for solution"
    },
    "PSRDOP": {
        "keywords": ["dop", "dilution of precision", "gdop", "pdop", "hdop"],
        "category": "signal",
        "description": "Dilution of precision values"
    },
    
    # Detection & Status Logs
    "RXSTATUS": {
        "keywords": ["receiver status", "rx status", "system status", "jamming", "spoofing", "antenna"],
        "category": "detection",
        "description": "Receiver status word with error/warning bits"
    },
    "RXSTATUSEVENT": {
        "keywords": ["status event", "receiver event", "status change"],
        "category": "detection",
        "description": "Receiver status change events"
    },
    "ITDETECTSTATUS": {
        "keywords": ["interference", "rf interference", "spectrum", "interference detection"],
        "category": "detection",
        "description": "Interference detection status"
    },
    
    # Time & Clock Logs
    "TIME": {
        "keywords": ["time", "clock", "utc", "gps time", "time offset"],
        "category": "time",
        "description": "Receiver time and clock information"
    },
    "PPSCONTROL": {
        "keywords": ["pps", "pulse per second", "timing pulse"],
        "category": "time",
        "description": "PPS output control"
    },
    
    # System & Configuration Logs
    "VERSION": {
        "keywords": ["version", "firmware", "software version", "hardware version"],
        "category": "system",
        "description": "Receiver version information"
    },
    "HWMONITOR": {
        "keywords": ["hardware monitor", "temperature", "voltage", "hw status"],
        "category": "system",
        "description": "Hardware monitoring (temperature, voltage, etc.)"
    },
    "MODELFEATURES": {
        "keywords": ["model features", "receiver features", "capabilities"],
        "category": "system",
        "description": "Receiver model and features"
    },
    "VALIDMODELS": {
        "keywords": ["valid models", "authorized models"],
        "category": "system",
        "description": "Valid receiver models"
    },
    "AUTHCODES": {
        "keywords": ["auth codes", "authorization", "license"],
        "category": "system",
        "description": "Authorization codes"
    },
    
    # Communication & Port Logs
    "PORTSTATS": {
        "keywords": ["port statistics", "port stats", "communication stats"],
        "category": "communication",
        "description": "Communication port statistics"
    },
    "SERIALCONFIG": {
        "keywords": ["serial config", "serial port", "baud rate"],
        "category": "communication",
        "description": "Serial port configuration"
    },
    "INTERFACEMODE": {
        "keywords": ["interface mode", "port mode"],
        "category": "communication",
        "description": "Interface mode configuration"
    },
    "ICOMCONFIG": {
        "keywords": ["icom config", "internet communication"],
        "category": "communication",
        "description": "Internet communication configuration"
    },
    "IPCONFIG": {
        "keywords": ["ip config", "network config", "ip address"],
        "category": "communication",
        "description": "IP configuration"
    },
    "DNSCONFIG": {
        "keywords": ["dns config", "dns server"],
        "category": "communication",
        "description": "DNS configuration"
    },
    "NTRIPCONFIG": {
        "keywords": ["ntrip", "ntrip config", "rtk corrections"],
        "category": "communication",
        "description": "NTRIP configuration"
    },
    
    # File System Logs
    "FILESYSTEMCAPACITY": {
        "keywords": ["file system", "storage", "disk space", "capacity"],
        "category": "filesystem",
        "description": "File system capacity"
    },
    "FILELIST": {
        "keywords": ["file list", "files", "directory"],
        "category": "filesystem",
        "description": "List of files"
    },
    "FILESYSTEMSTATUS": {
        "keywords": ["file system status", "storage status"],
        "category": "filesystem",
        "description": "File system status"
    },
    "FILETRANSFERSTATUS": {
        "keywords": ["file transfer", "transfer status", "upload", "download"],
        "category": "filesystem",
        "description": "File transfer status"
    },
    "FILEROTATECONFIG": {
        "keywords": ["file rotation", "log rotation"],
        "category": "filesystem",
        "description": "File rotation configuration"
    },
    
    # Logging Configuration
    "LOGLIST": {
        "keywords": ["log list", "active logs", "logging"],
        "category": "logging",
        "description": "List of active logs"
    },
    
    # Correction Services
    "TERRASTAR": {
        "keywords": ["terrastar", "ppp", "precise point positioning"],
        "category": "corrections",
        "description": "TerraStar correction service status"
    },
    "TERRASTARSTATUS": {
        "keywords": ["terrastar status", "ppp status"],
        "category": "corrections",
        "description": "TerraStar service status"
    },
    "OCEANIXINFO": {
        "keywords": ["oceanix", "oceanix info"],
        "category": "corrections",
        "description": "OceanIX correction service info"
    },
    "OCEANIXSTATUS": {
        "keywords": ["oceanix status"],
        "category": "corrections",
        "description": "OceanIX service status"
    },
    "VERIPOSINFO": {
        "keywords": ["veripos", "veripos info"],
        "category": "corrections",
        "description": "Veripos correction service info"
    },
    "VERIPOSDECODERSTATUS": {
        "keywords": ["veripos decoder", "veripos status"],
        "category": "corrections",
        "description": "Veripos decoder status"
    },
    
    # L-Band Logs
    "LBANDTRACKSTAT": {
        "keywords": ["lband", "l-band", "lband tracking"],
        "category": "lband",
        "description": "L-band tracking status"
    },
    "LBANDBEAMTABLE": {
        "keywords": ["lband beam", "beam table"],
        "category": "lband",
        "description": "L-band beam table"
    },
    "ASSIGNLBANDBEAM": {
        "keywords": ["assign lband", "lband assignment"],
        "category": "lband",
        "description": "L-band beam assignment"
    },
    
    # Radio Logs
    "SATEL4INFO": {
        "keywords": ["satel", "radio", "satel4"],
        "category": "radio",
        "description": "Satel radio information"
    },
    "SATEL9INFO": {
        "keywords": ["satel9", "radio info"],
        "category": "radio",
        "description": "Satel 9 radio information"
    },
    "SATEL4CONFIG": {
        "keywords": ["satel config", "radio config"],
        "category": "radio",
        "description": "Satel radio configuration"
    },
    "SATELSTATUS": {
        "keywords": ["satel status", "radio status"],
        "category": "radio",
        "description": "Satel radio status"
    },
    "SATELDETECT": {
        "keywords": ["satel detect", "radio detect"],
        "category": "radio",
        "description": "Satel radio detection"
    },
    
    # WiFi Logs
    "WIFIMODE": {
        "keywords": ["wifi", "wifi mode", "wireless"],
        "category": "wifi",
        "description": "WiFi mode"
    },
    "WIFIAPCHANNEL": {
        "keywords": ["wifi channel", "ap channel"],
        "category": "wifi",
        "description": "WiFi AP channel"
    },
    "WIFIAPSETTINGS": {
        "keywords": ["wifi settings", "ap settings", "ssid"],
        "category": "wifi",
        "description": "WiFi AP settings"
    },
    "WIFIALIGNAUTOMATION": {
        "keywords": ["wifi align", "wifi automation"],
        "category": "wifi",
        "description": "WiFi alignment automation"
    },
    
    # INS Configuration
    "CONNECTIMU": {
        "keywords": ["connect imu", "imu connection"],
        "category": "ins_config",
        "description": "IMU connection"
    },
    "INSCOMMAND": {
        "keywords": ["ins command", "ins control"],
        "category": "ins_config",
        "description": "INS command"
    },
    "INSCONFIG": {
        "keywords": ["ins config", "ins configuration"],
        "category": "ins_config",
        "description": "INS configuration"
    },
    "SETINSROTATION": {
        "keywords": ["ins rotation", "rotation offset"],
        "category": "ins_config",
        "description": "INS rotation offset"
    },
    "SETINSTRANSLATION": {
        "keywords": ["ins translation", "translation offset"],
        "category": "ins_config",
        "description": "INS translation offset"
    },
    "SETINSOFFSET": {
        "keywords": ["ins offset"],
        "category": "ins_config",
        "description": "INS offset"
    },
    "SETIMUORIENTATION": {
        "keywords": ["imu orientation"],
        "category": "ins_config",
        "description": "IMU orientation"
    },
    "VEHICLEBODYROTATION": {
        "keywords": ["vehicle rotation", "body rotation"],
        "category": "ins_config",
        "description": "Vehicle body rotation"
    },
    
    # Antenna Configuration
    "THISANTENNATYPE": {
        "keywords": ["antenna type", "this antenna"],
        "category": "antenna",
        "description": "This antenna type"
    },
    "BASEANTENNATYPE": {
        "keywords": ["base antenna", "base antenna type"],
        "category": "antenna",
        "description": "Base antenna type"
    },
    "RTKANTENNA": {
        "keywords": ["rtk antenna"],
        "category": "antenna",
        "description": "RTK antenna configuration"
    },
    "BASEANTENNAPCO": {
        "keywords": ["base antenna pco", "phase center offset"],
        "category": "antenna",
        "description": "Base antenna phase center offset"
    },
    "THISANTENNAPCO": {
        "keywords": ["this antenna pco"],
        "category": "antenna",
        "description": "This antenna phase center offset"
    },
    "HEADINGOFFSET": {
        "keywords": ["heading offset"],
        "category": "antenna",
        "description": "Heading offset"
    },
    "DUALANTENNAALIGN": {
        "keywords": ["dual antenna align", "antenna alignment"],
        "category": "antenna",
        "description": "Dual antenna alignment"
    },
    
    # SBAS Configuration
    "SBASCONTROL": {
        "keywords": ["sbas", "sbas control", "waas", "egnos"],
        "category": "sbas",
        "description": "SBAS control"
    },
    
    # RTK Configuration
    "GENERATERTK": {
        "keywords": ["generate rtk", "rtk generation"],
        "category": "rtk",
        "description": "RTK generation"
    },
    "GENERATEDIFF": {
        "keywords": ["generate diff", "differential"],
        "category": "rtk",
        "description": "Differential generation"
    },
    
    # Position Averaging
    "AUTOSURVEY": {
        "keywords": ["auto survey", "position survey"],
        "category": "survey",
        "description": "Automatic position survey"
    },
    "POSAVE": {
        "keywords": ["position average", "posave"],
        "category": "survey",
        "description": "Position averaging"
    },
    
    # Elevation & Cutoff
    "ELEVATIONCUTOFF": {
        "keywords": ["elevation cutoff", "cutoff angle"],
        "category": "config",
        "description": "Elevation cutoff angle"
    },
    "CUTOFF": {
        "keywords": ["cutoff"],
        "category": "config",
        "description": "Cutoff configuration"
    },
    
    # NMEA Configuration
    "NMEATALKER": {
        "keywords": ["nmea talker"],
        "category": "nmea",
        "description": "NMEA talker ID"
    },
    "NMEABEIDOUTALKER": {
        "keywords": ["nmea beidou", "beidou talker"],
        "category": "nmea",
        "description": "NMEA BeiDou talker"
    },
    "NMEAFORMAT": {
        "keywords": ["nmea format"],
        "category": "nmea",
        "description": "NMEA format"
    },
    "GGAQUALITY": {
        "keywords": ["gga quality"],
        "category": "nmea",
        "description": "GGA quality mapping"
    },
    
    # Interference Mitigation
    "ITPROGFILTBANK": {
        "keywords": ["interference filter", "programmable filter"],
        "category": "interference",
        "description": "Programmable interference filter bank"
    },
    "ITBANDPASSFILTBANK": {
        "keywords": ["bandpass filter"],
        "category": "interference",
        "description": "Bandpass filter bank"
    },
    "ITFILTTABLE": {
        "keywords": ["filter table"],
        "category": "interference",
        "description": "Interference filter table"
    },
    "ITSPECTRALANALYSIS": {
        "keywords": ["spectral analysis", "spectrum"],
        "category": "interference",
        "description": "Spectral analysis"
    },
    "ITFRONTENDMODE": {
        "keywords": ["frontend mode"],
        "category": "interference",
        "description": "Frontend mode"
    },
    "ITDETECTCONFIG": {
        "keywords": ["interference detect config"],
        "category": "interference",
        "description": "Interference detection configuration"
    },
    "ITPSDFINAL": {
        "keywords": ["psd final", "power spectral density"],
        "category": "interference",
        "description": "Final power spectral density"
    },
    "ITPSDDETECT": {
        "keywords": ["psd detect"],
        "category": "interference",
        "description": "PSD detection"
    },
    
    # Spoofing Detection
    "SKDETECT": {
        "keywords": ["spoofing detect", "sk detect"],
        "category": "spoofing",
        "description": "Spoofing detection"
    },
    "SKCALIBRATESTATUS": {
        "keywords": ["spoofing calibrate", "sk calibrate"],
        "category": "spoofing",
        "description": "Spoofing calibration status"
    },
    
    # Channel Configuration
    "CHANCONFIGLIST": {
        "keywords": ["channel config", "channel list"],
        "category": "channels",
        "description": "Channel configuration list"
    },
    
    # Software Load
    "SOFTLOADSTATUS": {
        "keywords": ["software load", "firmware load"],
        "category": "system",
        "description": "Software load status"
    },
    
    # Alignment
    "ALIGNAUTOMATION": {
        "keywords": ["align automation", "alignment"],
        "category": "alignment",
        "description": "Alignment automation"
    },
    
    # GIII Receiver Logs (for GIII receivers)
    "GIIICARDSTATUS": {
        "keywords": ["giii card", "card status"],
        "category": "giii",
        "description": "GIII card status"
    },
    "GIIITIMESOLUTION": {
        "keywords": ["giii time"],
        "category": "giii",
        "description": "GIII time solution"
    },
    "GIIIMEASUREMENTDATA": {
        "keywords": ["giii measurement"],
        "category": "giii",
        "description": "GIII measurement data"
    },
    "GIIISATPOS": {
        "keywords": ["giii satellite position"],
        "category": "giii",
        "description": "GIII satellite position"
    },
    "GIIIRXCOMMANDS": {
        "keywords": ["giii commands"],
        "category": "giii",
        "description": "GIII receiver commands"
    },
    "GIIIAGCINFO": {
        "keywords": ["giii agc"],
        "category": "giii",
        "description": "GIII AGC information"
    },
    "GIIIVERSION": {
        "keywords": ["giii version"],
        "category": "giii",
        "description": "GIII version"
    },
    "GIIIETHERNETSTATUS": {
        "keywords": ["giii ethernet"],
        "category": "giii",
        "description": "GIII Ethernet status"
    },
}

def generate_comprehensive_config():
    """Generate comprehensive configuration with ALL logs."""
    
    # Start with existing use cases (keep the detailed field mappings)
    use_cases = {}
    
    # Keep existing detailed use cases from enhanced config
    # (These have specific field indices and are fully mapped)
    existing_use_cases = [
        "height_analysis", "latitude_analysis", "longitude_analysis",
        "horizontal_accuracy", "vertical_accuracy",
        "satellites_tracked", "satellites_used",
        "velocity_analysis", "track_over_ground",
        "signal_quality", "scintillation_analysis",
        "jamming_detection", "spoofing_detection", "antenna_error",
        "interference_detection",
        "roll_analysis", "pitch_analysis", "azimuth_analysis",
        "list_logs", "time_range", "data_gaps",
        "receiver_status_summary", "tracking_status_summary", "position_summary"
    ]
    
    # Load existing detailed mappings
    import sys
    sys.path.insert(0, 'src')
    from generate_use_cases_from_logs import generate_enhanced_use_cases
    existing_config = generate_enhanced_use_cases()
    use_cases = existing_config['use_cases']
    
    # Add generic log type mappings for ALL other logs
    for log_name, log_info in ALL_LOGS.items():
        # Create a generic use case for this log if not already covered
        use_case_name = f"{log_name.lower()}_query"
        
        # Skip if we already have detailed mapping
        if any(uc.get('log_type') == log_name for uc in use_cases.values()):
            continue
        
        use_cases[use_case_name] = {
            "keywords": log_info["keywords"] + [log_name.lower(), log_name.lower().replace("_", " ")],
            "log_type": log_name,
            "field_index": None,  # Generic - no specific field
            "bit_position": None,
            "analysis_type": "raw_listing",  # Default to listing
            "description": log_info["description"],
            "unit": None,
            "category": log_info["category"]
        }
    
    # Enhanced fallback mappings with ALL categories
    fallback_mappings = {
        "detection": ["RXSTATUS", "ITDETECTSTATUS", "RXSTATUSEVENT", "SKDETECT"],
        "position": ["BESTPOS", "BESTGNSSPOS", "PSRPOS", "AVEPOS", "MARKPOS"],
        "velocity": ["BESTVEL", "BESTGNSSVEL", "PSRVEL"],
        "signal": ["TRACKSTAT", "SATVIS2", "BESTSATS", "RANGE", "PSRDOP"],
        "time": ["TIME", "CLOCKSTEERING", "TIMESYNC", "PPSCONTROL"],
        "ins": ["INSPVA", "INSPVAX", "INSPOSX", "MARKPVA"],
        "heading": ["HEADING2", "HEADING", "DUALANTENNAHEADING"],
        "system": ["VERSION", "HWMONITOR", "MODELFEATURES", "VALIDMODELS", "AUTHCODES", "SOFTLOADSTATUS"],
        "communication": ["PORTSTATS", "SERIALCONFIG", "INTERFACEMODE", "ICOMCONFIG", "IPCONFIG", "DNSCONFIG", "NTRIPCONFIG"],
        "filesystem": ["FILESYSTEMCAPACITY", "FILELIST", "FILESYSTEMSTATUS", "FILETRANSFERSTATUS", "FILEROTATECONFIG"],
        "corrections": ["TERRASTAR", "TERRASTARSTATUS", "OCEANIXINFO", "OCEANIXSTATUS", "VERIPOSINFO", "VERIPOSDECODERSTATUS"],
        "lband": ["LBANDTRACKSTAT", "LBANDBEAMTABLE", "ASSIGNLBANDBEAM"],
        "radio": ["SATEL4INFO", "SATEL9INFO", "SATEL4CONFIG", "SATELSTATUS", "SATELDETECT"],
        "wifi": ["WIFIMODE", "WIFIAPCHANNEL", "WIFIAPSETTINGS", "WIFIALIGNAUTOMATION"],
        "antenna": ["THISANTENNATYPE", "BASEANTENNATYPE", "RTKANTENNA", "BASEANTENNAPCO", "THISANTENNAPCO", "HEADINGOFFSET", "DUALANTENNAALIGN"],
        "interference": ["ITPROGFILTBANK", "ITBANDPASSFILTBANK", "ITFILTTABLE", "ITSPECTRALANALYSIS", "ITFRONTENDMODE", "ITDETECTCONFIG", "ITPSDFINAL", "ITPSDDETECT"],
        "ins_config": ["CONNECTIMU", "INSCOMMAND", "INSCONFIG", "SETINSROTATION", "SETINSTRANSLATION", "SETINSOFFSET", "SETIMUORIENTATION", "VEHICLEBODYROTATION"],
        "sbas": ["SBASCONTROL"],
        "rtk": ["GENERATERTK", "GENERATEDIFF"],
        "survey": ["AUTOSURVEY", "POSAVE"],
        "nmea": ["NMEATALKER", "NMEABEIDOUTALKER", "NMEAFORMAT", "GGAQUALITY"],
        "spoofing": ["SKDETECT", "SKCALIBRATESTATUS"],
        "channels": ["CHANCONFIGLIST"],
        "alignment": ["ALIGNAUTOMATION"],
        "giii": ["GIIICARDSTATUS", "GIIITIMESOLUTION", "GIIIMEASUREMENTDATA", "GIIISATPOS", "GIIIRXCOMMANDS", "GIIIAGCINFO", "GIIIVERSION", "GIIIETHERNETSTATUS"],
        "logging": ["LOGLIST"],
        "config": ["ELEVATIONCUTOFF", "CUTOFF"],
    }
    
    return {"use_cases": use_cases, "fallback_mappings": fallback_mappings}

if __name__ == "__main__":
    config = generate_comprehensive_config()
    
    # Write to YAML file
    with open("src/use_cases_config_comprehensive.yaml", "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    print(f"✅ Generated COMPREHENSIVE use cases configuration")
    print(f"✅ Total use cases: {len(config['use_cases'])}")
    print(f"✅ Total keywords: {sum(len(uc.get('keywords', [])) for uc in config['use_cases'].values())}")
    print(f"✅ Total log types covered: {len(set(uc.get('log_type') for uc in config['use_cases'].values() if uc.get('log_type')))}")
    print(f"✅ Total fallback categories: {len(config['fallback_mappings'])}")
    print(f"✅ Saved to: src/use_cases_config_comprehensive.yaml")
    
    # Print summary by category
    print("\n📊 Coverage by Category:")
    categories = {}
    for uc in config['use_cases'].values():
        cat = uc.get('category', 'other')
        categories[cat] = categories.get(cat, 0) + 1
    
    for cat, count in sorted(categories.items()):
        print(f"  - {cat}: {count} use cases")
