#!/usr/bin/env python3
"""
ConvaBI License Generator Tool
External tool for generating ConvaBI license codes

This tool should be used by developers to generate license codes for ConvaBI.
It's separate from the Django application for security reasons.

Usage:
    python license_generator.py --type creator --count 10
    python license_generator.py --type viewer --count 5 --days 365
    python license_generator.py --generate-batch --file licenses.json
"""

import argparse
import hashlib
import secrets
import json
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional


class ConvaBILicenseGenerator:
    """ConvaBI License Code Generator"""
    
    def __init__(self):
        # Secret key for license generation (should be kept secure)
        self.generation_secret = "convabi-license-validation-secret-key-2024"
        self.version = "1.0.0"
    
    def generate_license_code(self, license_type: str = 'creator', seed: str = None) -> str:
        """
        Generate a 16-character license code
        
        Args:
            license_type: Type of license ('creator' or 'viewer')
            seed: Optional seed for deterministic generation
            
        Returns:
            16-character license code
        """
        if seed is None:
            seed = secrets.token_hex(8)
        
        # Create a hash based on the seed, license type, and secret
        hash_input = f"{seed}{license_type}{self.generation_secret}"
        hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()
        
        # Extract alphanumeric characters from hash
        license_code = ""
        for char in hash_digest:
            if len(license_code) >= 16:
                break
            if char.isalnum():
                license_code += char.upper()
        
        # Pad with random characters if needed
        while len(license_code) < 16:
            license_code += secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        
        return license_code[:16]
    
    def generate_batch_licenses(self, license_type: str, count: int, 
                               max_users: int = 1, valid_days: int = None) -> List[Dict]:
        """
        Generate multiple license codes
        
        Args:
            license_type: Type of license ('creator' or 'viewer')
            count: Number of licenses to generate
            max_users: Maximum users per license
            valid_days: Number of days the license is valid (None = no expiration)
            
        Returns:
            List of license data dictionaries
        """
        licenses = []
        
        for i in range(count):
            license_code = self.generate_license_code(license_type, f"batch_{i}_{secrets.token_hex(4)}")
            
            # Calculate expiration date if specified
            valid_until = None
            if valid_days:
                valid_until = (datetime.now() + timedelta(days=valid_days)).isoformat()
            
            license_data = {
                'license_code': license_code,
                'license_type': license_type,
                'max_users': max_users,
                'valid_until': valid_until,
                'description': f'Batch generated {license_type} license #{i+1}',
                'generated_at': datetime.now().isoformat(),
                'generated_by': 'ConvaBI License Generator v' + self.version,
            }
            
            licenses.append(license_data)
        
        return licenses
    
    def validate_license_format(self, license_code: str) -> bool:
        """
        Validate license code format
        
        Args:
            license_code: License code to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        if not license_code or len(license_code) != 16:
            return False
        
        # Check if all characters are alphanumeric uppercase
        return license_code.isalnum() and license_code.isupper()
    
    def save_licenses_to_file(self, licenses: List[Dict], filename: str) -> None:
        """
        Save generated licenses to a JSON file
        
        Args:
            licenses: List of license data dictionaries
            filename: Output filename
        """
        output_data = {
            'generator_version': self.version,
            'generated_at': datetime.now().isoformat(),
            'total_licenses': len(licenses),
            'licenses': licenses
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Saved {len(licenses)} licenses to {filename}")
    
    def load_licenses_from_file(self, filename: str) -> List[Dict]:
        """
        Load licenses from a JSON file
        
        Args:
            filename: Input filename
            
        Returns:
            List of license data dictionaries
        """
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            
            return data.get('licenses', [])
        except FileNotFoundError:
            print(f"Error: File {filename} not found")
            return []
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in file {filename}")
            return []
    
    def print_license_summary(self, licenses: List[Dict]) -> None:
        """
        Print a summary of generated licenses
        
        Args:
            licenses: List of license data dictionaries
        """
        print("\n" + "="*60)
        print("CONVABI LICENSE GENERATION SUMMARY")
        print("="*60)
        print(f"Total licenses generated: {len(licenses)}")
        print(f"Generation time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Generator version: {self.version}")
        print("-"*60)
        
        # Group by license type
        type_counts = {}
        for license in licenses:
            license_type = license['license_type']
            type_counts[license_type] = type_counts.get(license_type, 0) + 1
        
        for license_type, count in type_counts.items():
            print(f"{license_type.title()} licenses: {count}")
        
        print("-"*60)
        print("Sample license codes:")
        for i, license in enumerate(licenses[:5]):  # Show first 5
            print(f"  {i+1}. {license['license_code']} ({license['license_type']})")
        
        if len(licenses) > 5:
            print(f"  ... and {len(licenses) - 5} more")
        
        print("="*60)
        print("IMPORTANT SECURITY NOTES:")
        print("- Keep these license codes secure")
        print("- Only share with authorized administrators")
        print("- Each license code is unique and cannot be regenerated")
        print("- Use the ConvaBI admin interface to assign licenses to users")
        print("="*60)


def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(
        description='ConvaBI License Generator Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10 creator licenses
  python license_generator.py --type creator --count 10

  # Generate 5 viewer licenses valid for 1 year
  python license_generator.py --type viewer --count 5 --days 365

  # Generate licenses and save to file
  python license_generator.py --type creator --count 10 --output licenses.json

  # Generate batch with custom settings
  python license_generator.py --type creator --count 10 --max-users 5 --days 180 --output batch_licenses.json
        """
    )
    
    parser.add_argument(
        '--type', 
        choices=['creator', 'viewer'], 
        default='creator',
        help='License type (default: creator)'
    )
    
    parser.add_argument(
        '--count', 
        type=int, 
        default=1,
        help='Number of licenses to generate (default: 1)'
    )
    
    parser.add_argument(
        '--max-users', 
        type=int, 
        default=1,
        help='Maximum users per license (default: 1)'
    )
    
    parser.add_argument(
        '--days', 
        type=int,
        help='Number of days the license is valid (default: no expiration)'
    )
    
    parser.add_argument(
        '--output', 
        type=str,
        help='Output filename for generated licenses (JSON format)'
    )
    
    parser.add_argument(
        '--validate', 
        type=str,
        help='Validate a license code format'
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version='ConvaBI License Generator v1.0.0'
    )
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ConvaBILicenseGenerator()
    
    # Handle validation
    if args.validate:
        is_valid = generator.validate_license_format(args.validate)
        print(f"License code '{args.validate}' is {'VALID' if is_valid else 'INVALID'}")
        sys.exit(0 if is_valid else 1)
    
    # Validate arguments
    if args.count <= 0:
        print("Error: Count must be greater than 0")
        sys.exit(1)
    
    if args.count > 10000:
        print("Error: Cannot generate more than 10,000 licenses at once")
        sys.exit(1)
    
    if args.max_users <= 0:
        print("Error: Max users must be greater than 0")
        sys.exit(1)
    
    if args.days is not None and args.days <= 0:
        print("Error: Days must be greater than 0")
        sys.exit(1)
    
    # Generate licenses
    print(f"Generating {args.count} {args.type} license(s)...")
    
    licenses = generator.generate_batch_licenses(
        license_type=args.type,
        count=args.count,
        max_users=args.max_users,
        valid_days=args.days
    )
    
    # Save to file if specified
    if args.output:
        generator.save_licenses_to_file(licenses, args.output)
    
    # Print summary
    generator.print_license_summary(licenses)
    
    # Show individual license codes if count is small
    if args.count <= 20 and not args.output:
        print("\nGenerated license codes:")
        for i, license in enumerate(licenses):
            expiry = f" (expires: {license['valid_until'][:10]})" if license['valid_until'] else ""
            print(f"  {i+1:2d}. {license['license_code']}{expiry}")


if __name__ == '__main__':
    main() 