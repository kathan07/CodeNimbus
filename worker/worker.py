import redis
import json
import subprocess
import os
import tempfile
import shutil
import time
import resource
import signal
import sys
import urllib.parse
import requests

class SecuritySandbox:
    @staticmethod
    def limit_resources():
        # Set strict resource limits
        resource.setrlimit(resource.RLIMIT_CPU, (5, 5))  # 5 seconds CPU time
        resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))  # 256MB memory
        resource.setrlimit(resource.RLIMIT_FSIZE, (10 * 1024 * 1024, 10 * 1024 * 1024))  # 10MB file size
    
    @staticmethod
    def prevent_syscalls(signum, frame):
        # Prevent system calls
        sys.exit(1)

class CodeExecutor:
    @staticmethod
    def download_file(file_url: str, local_path: str):
        """
        Download a file from a given URL to a local path
        """
        response = requests.get(file_url)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        return local_path

    @staticmethod
    def prepare_execution_environment(code_url, input_url, output_url):
        # Create a secure temporary directory
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Determine file extensions
            code_ext = os.path.splitext(code_url)[1]
            
            # Prepare paths
            code_path = os.path.join(temp_dir, f'solution{code_ext}')
            input_path = os.path.join(temp_dir, 'input.txt')
            output_path = os.path.join(temp_dir, 'output.txt')
            executable_path = os.path.join(temp_dir, 'solution')
            
            # Download files
            CodeExecutor.download_file(code_url, code_path)
            CodeExecutor.download_file(input_url, input_path)
            CodeExecutor.download_file(output_url, output_path)
            
            return temp_dir, code_path, executable_path, input_path, output_path
        
        except Exception as e:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    @staticmethod
    def execute_python_code(code_url, input_url, output_url):
        # Create secure execution environment
        temp_dir, code_path, _, input_path, output_path = CodeExecutor.prepare_execution_environment(
            code_url, input_url, output_url
        )
        
        try:
            # Prepare syscall prevention
            signal.signal(signal.SIGSYS, SecuritySandbox.prevent_syscalls)
            
            # Run the code with input and output files
            process = subprocess.Popen(
                ['python3', '-W', 'ignore', code_path],
                stdin=open(input_path, 'r'),
                stdout=open(output_path + '.actual', 'w'),
                stderr=subprocess.PIPE,
                preexec_fn=SecuritySandbox.limit_resources,
                text=True
            )
            
            try:
                # Wait for process to complete
                stdout, stderr = process.communicate(timeout=5)
                
                # Compare output files
                with open(output_path, 'r') as expected, open(output_path + '.actual', 'r') as actual:
                    expected_content = expected.read().strip()
                    actual_content = actual.read().strip()
                
                passed = expected_content == actual_content
                
                return {
                    'passed': passed,
                    'expected': expected_content,
                    'actual': actual_content,
                    'stderr': stderr
                }
            
            except subprocess.TimeoutExpired:
                process.kill()
                return {
                    'passed': False,
                    'error': 'Execution Timeout'
                }
        
        except Exception as e:
            return {
                'passed': False,
                'error': str(e)
            }
        
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def execute_c_code(code_url, input_url, output_url):
        # Create secure execution environment
        temp_dir, code_path, executable_path, input_path, output_path = CodeExecutor.prepare_execution_environment(
            code_url, input_url, output_url
        )
        
        try:
            # Compile the code with strict security flags
            compile_result = subprocess.run(
                ['gcc', 
                 '-O2',           # Optimization 
                 '-w',            # Suppress warnings
                 '-static',       # Static linking to reduce external dependencies
                 '-fno-stack-protector',  # Reduce potential exploit surface
                 '-Wl,-z,noexecstack',   # No executable stack
                 code_path, 
                 '-o', executable_path
                ],
                capture_output=True,
                text=True
            )
            
            # Check compilation
            if compile_result.returncode != 0:
                return {
                    'passed': False,
                    'error': 'Compilation Error',
                    'stderr': compile_result.stderr
                }
            
            # Prepare syscall prevention
            signal.signal(signal.SIGSYS, SecuritySandbox.prevent_syscalls)
            
            # Execute the compiled program
            process = subprocess.Popen(
                [executable_path],
                stdin=open(input_path, 'r'),
                stdout=open(output_path + '.actual', 'w'),
                stderr=subprocess.PIPE,
                preexec_fn=SecuritySandbox.limit_resources,
                text=True
            )
            
            try:
                # Wait for process to complete
                stdout, stderr = process.communicate(timeout=5)
                
                # Compare output files
                with open(output_path, 'r') as expected, open(output_path + '.actual', 'r') as actual:
                    expected_content = expected.read().strip()
                    actual_content = actual.read().strip()
                
                passed = expected_content == actual_content
                
                return {
                    'passed': passed,
                    'expected': expected_content,
                    'actual': actual_content,
                    'stderr': stderr
                }
            
            except subprocess.TimeoutExpired:
                process.kill()
                return {
                    'passed': False,
                    'error': 'Execution Timeout'
                }
        
        except Exception as e:
            return {
                'passed': False,
                'error': str(e)
            }
        
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def execute_cpp_code(code_url, input_url, output_url):
        # Create secure execution environment
        temp_dir, code_path, executable_path, input_path, output_path = CodeExecutor.prepare_execution_environment(
            code_url, input_url, output_url
        )
        
        try:
            # Compile the code with strict security flags
            compile_result = subprocess.run(
                ['g++', 
                 '-O2',           # Optimization 
                 '-w',            # Suppress warnings
                 '-static',       # Static linking to reduce external dependencies
                 '-fno-stack-protector',  # Reduce potential exploit surface
                 '-Wl,-z,noexecstack',   # No executable stack
                 code_path, 
                 '-o', executable_path
                ],
                capture_output=True,
                text=True
            )
            
            # Check compilation
            if compile_result.returncode != 0:
                return {
                    'passed': False,
                    'error': 'Compilation Error',
                    'stderr': compile_result.stderr
                }
            
            # Prepare syscall prevention
            signal.signal(signal.SIGSYS, SecuritySandbox.prevent_syscalls)
            
            # Execute the compiled program
            process = subprocess.Popen(
                [executable_path],
                stdin=open(input_path, 'r'),
                stdout=open(output_path + '.actual', 'w'),
                stderr=subprocess.PIPE,
                preexec_fn=SecuritySandbox.limit_resources,
                text=True
            )
            
            try:
                # Wait for process to complete
                stdout, stderr = process.communicate(timeout=5)
                
                # Compare output files
                with open(output_path, 'r') as expected, open(output_path + '.actual', 'r') as actual:
                    expected_content = expected.read().strip()
                    actual_content = actual.read().strip()
                
                passed = expected_content == actual_content
                
                return {
                    'passed': passed,
                    'expected': expected_content,
                    'actual': actual_content,
                    'stderr': stderr
                }
            
            except subprocess.TimeoutExpired:
                process.kill()
                return {
                    'passed': False,
                    'error': 'Execution Timeout'
                }
        
        except Exception as e:
            return {
                'passed': False,
                'error': str(e)
            }
        
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

def worker():
    # Parse Redis URL
    redis_url = os.environ.get('REDIS_URL', 'rediss://username:password@host:port')
    parsed_url = urllib.parse.urlparse(redis_url)
    redis_host = parsed_url.hostname
    redis_port = parsed_url.port or 6379
    redis_username = parsed_url.username or 'default'
    redis_password = parsed_url.password or ''

    # Create Redis client
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        username=redis_username,
        password=redis_password,
        ssl=True if parsed_url.scheme == 'rediss' else False,
        ssl_cert_reqs='none'
    )
    
    while True:
        try:
            # Block and wait for job
            _, job_data = redis_client.blpop('code_execution_queue')
            job = json.loads(job_data)
            
            try:
                # Validate job has all required URLs
                required_urls = ['code_file_url', 'input_file_url', 'output_file_url']
                if not all(f in job for f in required_urls):
                    raise ValueError("Missing required file URLs")
                
                # Execute code based on language
                if job['language'] == 'python':
                    result = CodeExecutor.execute_python_code(
                        job['code_file_url'], 
                        job['input_file_url'],
                        job['output_file_url']
                    )
                elif job['language'] == 'c':
                    result = CodeExecutor.execute_c_code(
                        job['code_file_url'], 
                        job['input_file_url'],
                        job['output_file_url']
                    )
                elif job['language'] == 'cpp':
                    result = CodeExecutor.execute_cpp_code(
                        job['code_file_url'], 
                        job['input_file_url'],
                        job['output_file_url']
                    )
                else:
                    raise ValueError(f"Unsupported language: {job['language']}")
                
                # Store results in Redis
                redis_client.set(
                    f'job_result:{job["job_id"]}', 
                    json.dumps({
                        'status': 'completed',
                        'result': result,
                        'passed': result.get('passed', False)
                    }),
                    ex=3600  # Expire after 1 hour
                )
            
            except Exception as e:
                redis_client.set(
                    f'job_result:{job["job_id"]}', 
                    json.dumps({
                        'status': 'error',
                        'error': str(e)
                    }),
                    ex=3600  # Expire after 1 hour
                )

        except Exception as e:
            # Log unexpected errors
            print(f"Unexpected error in worker: {e}")
            time.sleep(5)  # Avoid tight error loop

if __name__ == "__main__":
    worker()