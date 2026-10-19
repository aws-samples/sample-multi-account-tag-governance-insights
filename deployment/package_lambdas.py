#!/usr/bin/env python3
"""
Lambda Packaging Script for TagInsights

Creates deployment-ready Lambda packages by bundling the real TagInsights
implementation code with the handler wrappers and dependencies.
"""

import os
import sys
import shutil
import zipfile
import tempfile
import subprocess
import hashlib
import base64
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json


class LambdaPackager:
    """Packages TagInsights code for Lambda deployment."""
    
    def __init__(self, project_root: str = None):
        """
        Initialize the packager.
        
        Args:
            project_root: Root directory of the TagInsights project
        """
        self.project_root = Path(project_root or os.path.dirname(os.path.dirname(__file__)))
        self.deployment_dir = self.project_root / 'deployment'
        self.lambda_handlers_dir = self.project_root / 'src' / 'lambda_handlers'
        self.src_dir = self.project_root / 'src'
        self.build_dir = self.deployment_dir / 'lambda_builds'
        
        # Ensure build directory exists
        self.build_dir.mkdir(exist_ok=True)
        
        print(f"Lambda Packager initialized:")
        print(f"  Project root: {self.project_root}")
        print(f"  Build directory: {self.build_dir}")
    
    @staticmethod
    def _lambda_functions() -> Dict[str, Dict[str, str]]:
        """Return the supported Lambda packaging definitions."""
        return {
            'aggregator_coordinator': {
                'handler_file': 'aggregator_coordinator.py',
                'handler_name': 'lambda_handler',
                'description': 'Aggregator Coordinator for org-wide tag scanning via Resource Explorer'
            },
            'aggregator_worker': {
                'handler_file': 'aggregator_worker.py',
                'handler_name': 'lambda_handler',
                'description': 'Aggregator Worker for scanning individual accounts and writing tag deltas'
            },
            'analytics_writer': {
                'handler_file': 'analytics_writer_handler.py',
                'handler_name': 'lambda_handler',
                'description': 'Analytics Writer for converting regional JSON deltas to central S3 Parquet',
            },
            'monthly_aggregator': {
                'handler_file': 'monthly_aggregator_handler.py',
                'handler_name': 'lambda_handler',
                'description': 'Monthly Aggregator for consolidating 90-day-old data into monthly summaries'
            },
            'cost_analyzer_coordinator': {
                'handler_file': 'cost/cost_analyzer_coordinator_handler.py',
                'handler_name': 'lambda_handler',
                'description': 'Cost Analyzer Coordinator for querying changed resources and queueing cost analysis work'
            },
            'cost_analyzer_worker': {
                'handler_file': 'cost/cost_analyzer_worker_handler.py',
                'handler_name': 'lambda_handler',
                'description': 'Cost Analyzer Worker for processing resource batches and writing normalized tag costs'
            },
            'cost_deduplicator': {
                'handler_file': 'cost/cost_deduplicator_handler.py',
                'handler_name': 'lambda_handler',
                'description': 'Cost Deduplicator for consolidating daily partitions into monthly archives'
            }
        }

    def package_lambdas(
        self,
        function_names: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """Package only the requested Lambda functions."""
        lambda_functions = self._lambda_functions()
        selected_names = (
            list(lambda_functions)
            if function_names is None
            else function_names
        )
        unknown = [
            name
            for name in selected_names
            if name not in lambda_functions
        ]
        if unknown:
            raise ValueError(
                f"Unknown Lambda function selection: {', '.join(unknown)}"
            )

        print(
            f"\n=== Packaging {len(selected_names)} Lambda Function"
            f"{'s' if len(selected_names) != 1 else ''} ==="
        )
        package_paths = {}

        for func_name in selected_names:
            func_config = lambda_functions[func_name]
            try:
                print(f"\nPackaging {func_name}...")
                package_path = self.package_lambda_function(
                    function_name=func_name,
                    handler_file=func_config['handler_file'],
                    description=func_config['description'],
                    include_dirs=func_config.get('include_dirs'),
                )
                package_paths[func_name] = package_path
                print(f"✓ {func_name} packaged: {package_path}")
                
            except Exception as e:
                print(f"✗ Error packaging {func_name}: {e}")
                raise

        print(f"\n✅ Selected Lambda functions packaged successfully!")
        print(f"   Build directory: {self.build_dir}")
        return package_paths

    def package_all_lambdas(self) -> Dict[str, str]:
        """Package all supported Lambda functions."""
        return self.package_lambdas()
    
    def package_lambda_function(self,
                               function_name: str,
                               handler_file: str,
                               description: str = "",
                               include_dirs: Optional[List[str]] = None) -> str:
        """
        Package a single Lambda function.
        
        Args:
            function_name: Name of the Lambda function
            handler_file: Handler file name 
            description: Description of the function
            
        Returns:
            str: Path to the created ZIP package
        """
        package_path = self.build_dir / f"{function_name}.zip"
        package_path.unlink(missing_ok=True)

        try:
            # Create temporary directory for building
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Create package structure
                print(f"  Building package structure...")
                self._copy_source_code(temp_path)
                self._copy_handler_code(temp_path, handler_file, include_dirs)
                self._install_dependencies(temp_path)
                self._remove_generated_bytecode(temp_path)
                self._create_index_file(temp_path, handler_file)

                # Create ZIP package
                print(f"  Creating ZIP package...")
                self._create_zip_package(temp_path, package_path)

                # Validate package
                package_info = self._validate_package(package_path)
                print(f"  Package size: {package_info['size_mb']:.1f} MB")
                print(f"  Files included: {package_info['file_count']}")
        except Exception:
            package_path.unlink(missing_ok=True)
            raise
        
        return str(package_path)
    
    def _copy_source_code(self, temp_path: Path):
        """Copy TagInsights source code to package directory."""
        print(f"    Copying source code...")
        
        # Copy the entire src directory
        src_dest = temp_path / 'src'
        shutil.copytree(self.src_dir, src_dest, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        
        # Copy requirements.txt for reference
        requirements_src = self.project_root / 'requirements.txt'
        if requirements_src.exists():
            shutil.copy2(requirements_src, temp_path / 'requirements.txt')
    
    def _copy_handler_code(
        self,
        temp_path: Path,
        handler_file: str,
        include_dirs: Optional[List[str]] = None,
    ):
        """Copy Lambda handler code and any explicitly required modules."""
        include_dirs = include_dirs or []
        print(f"    Copying handler: {handler_file}")
        
        # Copy the specific handler file (may be in a subdirectory)
        handler_src = self.lambda_handlers_dir / handler_file
        if not handler_src.exists():
            raise FileNotFoundError(f"Handler file not found: {handler_src}")
        
        handler_path = Path(handler_file)
        if len(handler_path.parts) > 1:
            subdir_name = handler_path.parts[0]
            shutil.copytree(
                self.lambda_handlers_dir / subdir_name,
                temp_path / subdir_name,
                ignore=shutil.ignore_patterns('__pycache__', '*.pyc', 'spike'),
            )
        else:
            shutil.copy2(handler_src, temp_path / handler_path.name)

        for include_dir in include_dirs:
            include_src = self.lambda_handlers_dir / include_dir
            if not include_src.is_dir():
                raise FileNotFoundError(
                    f"Required include directory not found: {include_src}"
                )
            shutil.copytree(
                include_src,
                temp_path / include_dir,
                ignore=shutil.ignore_patterns('__pycache__', '*.pyc'),
            )
        
        # Copy the __init__.py file
        init_src = self.lambda_handlers_dir / '__init__.py'
        if init_src.exists():
            shutil.copy2(init_src, temp_path / '__init__.py')
    
    def _install_dependencies(self, temp_path: Path):
        """Install Lambda dependencies."""
        print(f"    Installing dependencies...")
        
        requirements_file = self.deployment_dir / 'requirements-lambda.txt'
        
        if requirements_file.exists():
            # Install dependencies directly to the package root for Lambda.
            subprocess.run([
                sys.executable, '-m', 'pip', 'install',
                '-r', str(requirements_file),
                '-t', str(temp_path),
                '--platform', 'manylinux2014_x86_64',
                '--python-version', '3.11',
                '--implementation', 'cp',
                '--abi', 'cp311',
                '--only-binary', ':all:',
                '--no-compile',
                '--quiet'
            ], check=True)

            print(f"    Dependencies installed to package root (Lambda accessible)")
        else:
            print(f"    No requirements file found, skipping dependency installation")

    def _remove_generated_bytecode(self, temp_path: Path):
        """Remove bytecode and cache directories from the package staging tree."""
        for cache_dir in list(temp_path.rglob('__pycache__')):
            if cache_dir.is_dir():
                shutil.rmtree(cache_dir)
        for suffix in ('*.pyc', '*.pyo'):
            for bytecode_file in temp_path.rglob(suffix):
                bytecode_file.unlink()
    
    def _create_index_file(self, temp_path: Path, handler_file: str):
        """Create index.py file that imports the handler."""
        print(f"    Creating index.py...")
        
        handler_module = ".".join(Path(handler_file).with_suffix("").parts)
        
        # Create index.py that imports the handler
        index_content = f'''"""
Lambda entry point for TagInsights.

This file imports the appropriate handler function to serve as the
Lambda entry point (index.lambda_handler).
"""

# Import the handler function
from {handler_module} import lambda_handler

# The lambda_handler function is now available as index.lambda_handler
# This allows CloudFormation to reference it as "index.lambda_handler"

# For debugging
if __name__ == "__main__":
    print("TagInsights Lambda handler loaded successfully")
    print(f"Handler module: {handler_module}")
'''
        
        index_path = temp_path / 'index.py'
        index_path.write_text(index_content)
    
    def _create_zip_package(self, temp_path: Path, package_path: Path):
        """Create ZIP package from temporary directory."""
        # Ensure the parent directory exists
        package_path.parent.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in temp_path.rglob('*'):
                if file_path.is_file():
                    # Calculate relative path from temp_path
                    relative_path = file_path.relative_to(temp_path)
                    zipf.write(file_path, relative_path)
    
    def _validate_package(self, package_path: Path) -> Dict:
        """Validate the created package."""
        if not package_path.exists():
            raise FileNotFoundError(f"Package not created: {package_path}")
        
        size_bytes = package_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        
        # Count files in package
        file_count = 0
        with zipfile.ZipFile(package_path, 'r') as zipf:
            file_count = len(zipf.namelist())
        
        # Warn if package is very large
        if size_mb > 50:
            print(f"    Warning: Package is large ({size_mb:.1f} MB)")
        
        return {
            'size_bytes': size_bytes,
            'size_mb': size_mb,
            'file_count': file_count
        }

    @staticmethod
    def _sha256(package_path: Path) -> str:
        """Return the SHA-256 digest for a package."""
        digest = hashlib.sha256()
        with package_path.open('rb') as package_file:
            for chunk in iter(lambda: package_file.read(1024 * 1024), b''):
                digest.update(chunk)
        return digest.hexdigest()

    def _git_revision(self, revision: str) -> str:
        """Resolve a Git commit or tree without changing repository state."""
        result = subprocess.run(
            ['git', 'rev-parse', revision],
            cwd=self.project_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    
    def upload_packages_to_s3(self, 
                             package_paths: Dict[str, str],
                             bucket_name: str,
                             deployment_version: str = None,
                             versioned_only: bool = False,
                             source_commit: str = None,
                             source_tree: str = None) -> Dict[str, str]:
        """
        Upload Lambda packages to S3 with versioned paths.
        
        Args:
            package_paths: Dict of function_name -> local_package_path
            bucket_name: S3 bucket name
            deployment_version: Version identifier for this deployment
            
        Returns:
            Dict[str, str]: Dict of function_name -> s3_url
        """
        if versioned_only and not deployment_version:
            raise ValueError(
                "deployment_version is required for versioned-only uploads"
            )
        if not source_tree:
            raise ValueError("source_tree is required for artifact uploads")

        source_commit = source_commit or self._git_revision('HEAD')
        print(f"\n=== Uploading packages to S3: {bucket_name} ===")
        
        import boto3
        s3_client = boto3.client('s3')
        
        s3_urls = {}
        
        for func_name, local_path in package_paths.items():
            try:
                package_path = Path(local_path)
                package_sha256 = self._sha256(package_path)
                checksum_sha256 = base64.b64encode(
                    bytes.fromhex(package_sha256)
                ).decode("ascii")
                put_args = {
                    'Bucket': bucket_name,
                    'ContentType': 'application/zip',
                    'ServerSideEncryption': 'AES256',
                    'ChecksumSHA256': checksum_sha256,
                    'IfNoneMatch': '*',
                    'Metadata': {
                        'source-commit': source_commit,
                        'source-tree': source_tree,
                        'sha256': package_sha256,
                    },
                }
                s3_url = None

                if deployment_version:
                    versioned_key = f"lambda-packages/v{deployment_version}/{func_name}.zip"
                    print(f"Uploading {func_name} (versioned: v{deployment_version})...")
                    with package_path.open('rb') as package_file:
                        s3_client.put_object(
                            **put_args,
                            Body=package_file,
                            Key=versioned_key,
                        )
                    print(f"✓ {func_name} uploaded: {versioned_key}")
                    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{versioned_key}"
                
                if not versioned_only:
                    latest_key = f"lambda-packages/{func_name}.zip"
                    with package_path.open('rb') as package_file:
                        s3_client.put_object(
                            **put_args,
                            Body=package_file,
                            Key=latest_key,
                        )
                    print(f"✓ {func_name} uploaded: {latest_key} (latest)")
                    if not s3_url:
                        s3_url = (
                            f"https://{bucket_name}.s3.amazonaws.com/"
                            f"{latest_key}"
                        )

                if s3_url is None:
                    raise ValueError(
                        "No S3 key selected; provide deployment_version or "
                        "disable versioned-only mode"
                    )

                if not deployment_version:
                    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{latest_key}"
                
                s3_urls[func_name] = s3_url
                
            except Exception as e:
                print(f"✗ Error uploading {func_name}: {e}")
                raise
        
        print(f"✅ All packages uploaded to S3!")
        if deployment_version:
            print(f"   Deployment version: v{deployment_version}")
        return s3_urls
    
    def create_deployment_manifest(
        self,
        package_paths: Dict[str, str],
        deployment_version: str,
        source_commit: str,
        source_tree: str,
        s3_urls: Dict[str, str] = None,
    ) -> str:
        """
        Create a deployment manifest with package information.
        
        Args:
            package_paths: Local package paths
            s3_urls: S3 URLs (optional)
            
        Returns:
            str: Path to manifest file
        """
        if not deployment_version:
            raise ValueError("deployment_version is required")
        if not source_commit or not source_tree:
            raise ValueError("source_commit and source_tree are required")

        manifest = {
            'timestamp': f"{__import__('datetime').datetime.now().isoformat()}",
            'version': '2.0.0',
            'deployment_version': deployment_version,
            'source_commit': source_commit,
            'source_tree': source_tree,
            'packages': {}
        }
        
        for func_name, local_path in package_paths.items():
            package_info = self._validate_package(Path(local_path))
            
            manifest['packages'][func_name] = {
                'local_path': local_path,
                'size_mb': package_info['size_mb'],
                'file_count': package_info['file_count'],
                'sha256': self._sha256(Path(local_path)),
                'intended_s3_key': (
                    f"lambda-packages/v{deployment_version}/"
                    f"{func_name}.zip"
                ),
            }
            
            if s3_urls and func_name in s3_urls:
                manifest['packages'][func_name]['s3_url'] = s3_urls[func_name]
        
        # Write manifest file
        manifest_path = self.build_dir / 'deployment_manifest.json'
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"\nDeployment manifest created: {manifest_path}")
        print(f"Deployment version: {deployment_version}")
        return str(manifest_path)
    
    def cleanup_build_directory(self):
        """Clean up build directory."""
        if self.build_dir.exists():
            shutil.rmtree(self.build_dir)
            print(f"Build directory cleaned: {self.build_dir}")


def sign_lambda_packages(
    package_paths: Dict[str, str],
    bucket_name: str,
    deployment_version: str,
    signing_profile_name: str = 'taginsights-lambda-signer'
) -> Dict[str, str]:
    """
    Sign Lambda packages using AWS Signer for production deployments.
    
    Code signing: only signed packages
    can be deployed to prod environments (CodeSigningConfigArn: Enforce).
    
    Prerequisites (one-time setup per region):
        aws signer put-signing-profile \\
            --profile-name taginsights-lambda-signer \\
            --platform-id AWSLambda-SHA384-ECDSA \\
            --signature-validity-period value=365,type=DAYS
    
    Args:
        package_paths: Dict of function_name -> local .zip path
        bucket_name: S3 bucket where unsigned packages were uploaded
        deployment_version: Immutable version shared by unsigned and signed keys
        signing_profile_name: AWS Signer profile name (default: taginsights-lambda-signer)
        
    Returns:
        Dict[str, str]: Dict of function_name -> signed S3 key
    """
    import boto3
    import time
    
    print(f"\n=== Signing Lambda packages with AWS Signer (profile: {signing_profile_name}) ===")
    
    signer = boto3.client('signer')
    s3 = boto3.client('s3')
    signed_keys = {}
    
    for func_name in package_paths.keys():
        unsigned_key = (
            f"lambda-packages/v{deployment_version}/{func_name}.zip"
        )
        signed_prefix = f"signed/v{deployment_version}/"
        
        print(f"Signing {func_name}...")
        try:
            # Start signing job: reads unsigned from lambda-packages/, writes signed/ to same bucket
            response = signer.start_signing_job(
                source={
                    's3': {
                        'bucketName': bucket_name,
                        'key': unsigned_key,
                        'version': 'null'  # latest version
                    }
                },
                destination={
                    's3': {
                        'bucketName': bucket_name,
                        'prefix': signed_prefix
                    }
                },
                profileName=signing_profile_name
            )
            
            job_id = response['jobId']
            print(f"  Signing job started: {job_id}")
            
            # Poll for completion (signing typically takes 5-15 seconds)
            max_wait = 120  # 2 minutes
            elapsed = 0
            while elapsed < max_wait:
                job = signer.describe_signing_job(jobId=job_id)
                status = job['status']
                
                if status == 'Succeeded':
                    signed_key = job['signedObject']['s3']['key']
                    
                    expected_key = (
                        f"signed/v{deployment_version}/{func_name}.zip"
                    )
                    if signed_key != expected_key:
                        s3.copy_object(
                            Bucket=bucket_name,
                            CopySource={'Bucket': bucket_name, 'Key': signed_key},
                            Key=expected_key
                        )
                        s3.delete_object(Bucket=bucket_name, Key=signed_key)
                        signed_key = expected_key
                    
                    signed_keys[func_name] = signed_key
                    print(f"  ✓ Signed: s3://{bucket_name}/{signed_key}")
                    break
                elif status == 'Failed':
                    reason = job.get('statusReason', 'Unknown')
                    print(f"  ✗ Signing FAILED for {func_name}: {reason}")
                    raise RuntimeError(f"Signing job failed for {func_name}: {reason}")
                else:
                    time.sleep(5)
                    elapsed += 5
            
            if elapsed >= max_wait:
                raise TimeoutError(f"Signing job timed out for {func_name} (job: {job_id})")
                
        except signer.exceptions.ResourceNotFoundException:
            print(f"  ✗ Signing profile '{signing_profile_name}' not found.")
            print(f"  Run: aws signer put-signing-profile --profile-name {signing_profile_name} --platform-id AWSLambda-SHA384-ECDSA")
            raise
        except Exception as e:
            print(f"  ✗ Error signing {func_name}: {e}")
            raise
    
    print(
        f"✅ All {len(signed_keys)} packages signed and uploaded to "
        f"signed/v{deployment_version}/"
    )
    return signed_keys


def main():
    """Main CLI interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Package TagInsights Lambda functions')
    parser.add_argument('--project-root', help='Project root directory')
    parser.add_argument('--upload-bucket', help='S3 bucket for uploading packages')
    parser.add_argument('--environment', default='dev',
                        choices=['dev', 'test', 'cert', 'uat', 'staging', 'prod'],
                        help='Deployment environment (prod enables code signing)')
    parser.add_argument('--signing-profile', default='taginsights-lambda-signer',
                        help='AWS Signer profile name (default: taginsights-lambda-signer)')
    parser.add_argument('--worker-only', action='store_true',
                        help='Package only cost_analyzer_worker')
    parser.add_argument('--version',
                        help='Explicit immutable deployment version')
    parser.add_argument('--versioned-only', action='store_true',
                        help='Never write unversioned lambda-packages/*.zip keys')
    parser.add_argument('--source-commit',
                        help='Source commit recorded in metadata and manifest')
    parser.add_argument('--source-tree',
                        help='Source tree recorded in the local manifest')
    parser.add_argument('--cleanup', action='store_true', help='Clean up build directory before packaging')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing packages')
    
    args = parser.parse_args()
    is_production = args.environment == 'prod'
    
    # Create packager
    packager = LambdaPackager(args.project_root)
    
    try:
        # Clean up if requested
        if args.cleanup:
            packager.cleanup_build_directory()
        
        if args.validate_only:
            # Just validate existing packages
            print("Validating existing packages...")
            for zip_file in packager.build_dir.glob('*.zip'):
                info = packager._validate_package(zip_file)
                print(f"  {zip_file.name}: {info['size_mb']:.1f} MB, {info['file_count']} files")
        else:
            if not args.version:
                raise ValueError("--version is required when packaging")
            if args.upload_bucket and not args.versioned_only:
                raise ValueError(
                    "--versioned-only is required for uploads; mutable "
                    "lambda-packages/*.zip publication is not supported"
                )
            selected = (
                ['cost_analyzer_worker']
                if args.worker_only
                else None
            )
            package_paths = packager.package_lambdas(selected)
            source_commit = (
                args.source_commit
                or packager._git_revision('HEAD')
            )
            source_tree = (
                args.source_tree
                or packager._git_revision('HEAD^{tree}')
            )
            
            # Upload to S3 if bucket specified
            s3_urls = {}
            if args.upload_bucket:
                s3_urls = packager.upload_packages_to_s3(
                    package_paths, 
                    args.upload_bucket,
                    deployment_version=args.version,
                    versioned_only=args.versioned_only,
                    source_commit=source_commit,
                    source_tree=source_tree,
                )
                
                # Production: sign packages after upload
                if is_production:
                    print(f"\n🔐 Production deployment detected — signing Lambda packages...")
                    print(f"   Prod Lambdas only accept packages signed by AWS Signer")
                    signed_keys = sign_lambda_packages(
                        package_paths=package_paths,
                        bucket_name=args.upload_bucket,
                        deployment_version=args.version,
                        signing_profile_name=args.signing_profile
                    )
                    print(
                        f"   ✓ {len(signed_keys)} packages signed in "
                        f"s3://{args.upload_bucket}/signed/v{args.version}/"
                    )
                    print(f"   CloudFormation will use signed/ prefix (CodeSigningConfigArn: Enforce)")
                else:
                    print(f"\n📦 Non-prod deployment ({args.environment}) — skipping code signing")
                    print(f"   To sign for prod, use: --environment prod --upload-bucket <bucket>")

            manifest_path = packager.create_deployment_manifest(
                package_paths,
                deployment_version=args.version,
                source_commit=source_commit,
                source_tree=source_tree,
                s3_urls=s3_urls,
            )
            
            print(f"\n🎉 Lambda packaging completed successfully!")
            print(f"   Environment: {args.environment}")
            print(f"   Code signing: {'✅ ENABLED (prod)' if is_production else '⚠️  DISABLED (non-prod)'}")
            print(f"   Packages: {len(package_paths)}")
            print(f"   Manifest: {manifest_path}")
            
            if s3_urls:
                print(f"   S3 uploads: {len(s3_urls)}")
    
    except Exception as e:
        print(f"\n❌ Packaging failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
