"""
Deterministic Mapping Engine for NovAtel Log Analysis

This module provides ZERO-LLM log and field selection using configuration-driven mappings.
All decisions are made via exact keyword matching and predefined rules.
"""

import yaml
import os
from typing import Dict, List, Optional, Tuple
from pathlib import Path

class DeterministicMapper:
    """
    Maps user questions to specific logs and fields using configuration file.
    NO LLM calls - pure Python logic.
    """
    
    def __init__(self, config_path: str = None):
        """Load use case configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent / "use_cases_config.yaml"
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.use_cases = self.config.get('use_cases', {})
        self.fallback_mappings = self.config.get('fallback_mappings', {})
        
        # Build reverse index: keyword → use_case_name for O(1) lookup
        self.keyword_index: Dict[str, str] = {}
        for use_case_name, use_case_data in self.use_cases.items():
            for keyword in use_case_data.get('keywords', []):
                # Store longest match first (prefer "signal quality" over "signal")
                if keyword not in self.keyword_index:
                    self.keyword_index[keyword.lower()] = use_case_name
        
        print(f"[MAPPER] Loaded {len(self.use_cases)} use cases, {len(self.keyword_index)} keywords")
    
    def extract_intent(self, question: str) -> Dict[str, any]:
        """
        Extract user intent from question using keyword matching.
        Returns: {
            'use_case': str,
            'log_type': str,
            'field_index': int | None,
            'bit_position': int | None,
            'analysis_type': str,
            'unit': str | None,
            'special_handler': str | None,
            'handler_name': str | None,  # For direct_handler type
            'confidence': float  # 1.0 = exact match, 0.5 = fallback
        }
        """
        q_lower = question.lower()
        
        # Step 1: Try exact keyword matching (longest match first)
        # Sort keywords by length descending to prefer "signal quality" over "signal"
        sorted_keywords = sorted(self.keyword_index.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if keyword in q_lower:
                use_case_name = self.keyword_index[keyword]
                use_case_data = self.use_cases[use_case_name]
                
                return {
                    'use_case': use_case_name,
                    'log_type': use_case_data.get('log_type'),
                    'field_index': use_case_data.get('field_index'),
                    'bit_position': use_case_data.get('bit_position'),
                    'analysis_type': use_case_data['analysis_type'],
                    'unit': use_case_data.get('unit'),
                    'description': use_case_data.get('description', ''),
                    'special_handler': use_case_data.get('special_handler'),
                    'handler_name': use_case_data.get('handler_name'),
                    'confidence': 1.0,
                    'match_type': 'exact_keyword',
                    'matched_keyword': keyword,
                }
        
        # Step 2: Fallback - infer category from question structure
        # Check for analysis type indicators
        analysis_type = self._infer_analysis_type(q_lower)
        category = self._infer_category(q_lower)
        
        if category and category in self.fallback_mappings:
            preferred_logs = self.fallback_mappings[category]
            
            return {
                'use_case': f'fallback_{category}',
                'log_type': preferred_logs[0],  # Use first preference
                'field_index': None,
                'bit_position': None,
                'analysis_type': analysis_type,
                'unit': None,
                'description': f'Fallback mapping for {category} category',
                'special_handler': None,
                'handler_name': None,
                'confidence': 0.5,
                'match_type': 'fallback_category',
                'matched_keyword': None,
            }
        
        # Step 3: No match found
        return {
            'use_case': 'unknown',
            'log_type': None,
            'field_index': None,
            'bit_position': None,
            'analysis_type': 'raw_listing',
            'unit': None,
            'description': 'No matching use case found',
            'special_handler': None,
            'handler_name': None,
            'confidence': 0.0,
            'match_type': 'no_match',
            'matched_keyword': None,
        }
    
    def _infer_analysis_type(self, question: str) -> str:
        """Infer analysis type from question structure."""
        # Detection/bit check indicators
        if any(kw in question for kw in ['detect', 'any', 'check', 'is there', 'do we have', 'occurred']):
            return 'bit_check'
        
        # Numeric statistics indicators
        if any(kw in question for kw in ['minimum', 'maximum', 'average', 'min', 'max', 'avg', 
                                          'highest', 'lowest', 'range', 'statistics', 'stats']):
            return 'numeric_stat'
        
        # Default to listing
        return 'raw_listing'
    
    def _infer_category(self, question: str) -> Optional[str]:
        """Infer log category from question content."""
        if any(kw in question for kw in ['position', 'coordinate', 'location', 'lat', 'lon', 'height', 'altitude']):
            return 'position'
        
        if any(kw in question for kw in ['velocity', 'speed', 'heading', 'direction']):
            return 'velocity'
        
        if any(kw in question for kw in ['signal', 'c/no', 'cno', 'carrier', 'tracking', 'satellite']):
            return 'signal'
        
        if any(kw in question for kw in ['time', 'clock', 'pps', 'sync']):
            return 'time'
        
        if any(kw in question for kw in ['detect', 'status', 'error', 'issue', 'problem', 'event']):
            return 'detection'
        
        return None
    
    def validate_against_file(self, intent: Dict, available_logs: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Validate that the required log type exists in the uploaded file.
        Returns: (is_valid, error_message)
        """
        required_log = intent.get('log_type')
        
        if not required_log:
            return False, "Could not determine which log type to use for this question."
        
        # Normalize log names (strip trailing A)
        normalized_available = [
            log[:-1] if (log.upper().endswith('A') and len(log) > 4) else log
            for log in available_logs
        ]
        
        normalized_required = (
            required_log[:-1] if (required_log.upper().endswith('A') and len(required_log) > 4) 
            else required_log
        )
        
        if normalized_required.upper() not in [l.upper() for l in normalized_available]:
            available_str = ", ".join(available_logs[:10])
            if len(available_logs) > 10:
                available_str += f", and {len(available_logs) - 10} more"
            
            return False, (
                f"The required log type `{required_log}` is not present in your file.\n\n"
                f"**Available log types:** {available_str}\n\n"
                f"**Suggestions:**\n"
                f"- Check if your file contains the data you're looking for\n"
                f"- Try asking about a different metric available in your logs\n"
                f"- Use 'list all logs' to see what's in your file"
            )
        
        return True, None
    
    def get_use_case_suggestions(self, available_logs: List[str]) -> List[str]:
        """
        Generate helpful suggestions based on what logs are available in the file.
        Returns list of example questions the user can ask.
        """
        suggestions = []
        
        # Normalize available logs
        normalized_logs = {
            (log[:-1] if (log.upper().endswith('A') and len(log) > 4) else log).upper(): log
            for log in available_logs
        }
        
        # Check which use cases are available
        for use_case_name, use_case_data in self.use_cases.items():
            log_type = use_case_data['log_type']
            normalized_log = (log_type[:-1] if (log_type.upper().endswith('A') and len(log_type) > 4) else log_type).upper()
            
            if normalized_log in normalized_logs:
                # Generate example question from first keyword
                keywords = use_case_data.get('keywords', [])
                if keywords:
                    example_keyword = keywords[0]
                    analysis_type = use_case_data['analysis_type']
                    
                    if analysis_type == 'bit_check':
                        suggestions.append(f"Check for {example_keyword}")
                    elif analysis_type == 'numeric_stat':
                        suggestions.append(f"What is the minimum {example_keyword}?")
                    else:
                        suggestions.append(f"Show {example_keyword} records")
        
        return suggestions[:8]  # Return top 8 suggestions
    
    def get_all_use_cases(self) -> Dict[str, Dict]:
        """Return all configured use cases for debugging/inspection."""
        return self.use_cases


# ═══════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════

_mapper_instance: Optional[DeterministicMapper] = None

def get_mapper() -> DeterministicMapper:
    """Get singleton mapper instance."""
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = DeterministicMapper()
    return _mapper_instance


# ═══════════════════════════════════════════════════════════════════
# TESTING / CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Test the mapper
    mapper = DeterministicMapper()
    
    test_questions = [
        "Do we have jamming in this file?",
        "Check for spoofing events",
        "What is the minimum height?",
        "Show me signal quality statistics",
        "Analyze scintillation",
        "List receiver status",
        "What is the maximum velocity?",
        "Any interference detected?",
    ]
    
    print("\n" + "="*70)
    print("DETERMINISTIC MAPPER TEST")
    print("="*70 + "\n")
    
    for question in test_questions:
        intent = mapper.extract_intent(question)
        print(f"Q: {question}")
        print(f"   → Use Case: {intent['use_case']}")
        print(f"   → Log: {intent['log_type']}, Field: {intent['field_index']}, Bit: {intent['bit_position']}")
        print(f"   → Analysis: {intent['analysis_type']}, Confidence: {intent['confidence']}")
        print(f"   → Match: {intent['match_type']} (keyword: {intent['matched_keyword']})")
        print()
