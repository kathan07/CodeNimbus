import os
import time
import redis
import paramiko
import json
import logging
from typing import List, Dict
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkerScalingManager:
    def __init__(self, 
                 redis_url: str, 
                 vm_ips: List[str], 
                 vm_credentials: Dict[str, Dict[str, str]],
                 worker_image: str = 'kathan07/worker',
                 queue_name: str = 'code_execution_queue',
                 jobs_per_worker: int = 5,
                 scaling_interval: int = 10):
        """
        Initialize scaling manager with configuration from environment variables
        
        :param redis_url: External Redis URL
        :param vm_ips: List of VM IP addresses
        :param vm_credentials: SSH credentials for each VM
        :param worker_image: Docker image for workers
        :param queue_name: Redis queue to monitor
        :param jobs_per_worker: Number of jobs per worker before scaling
        :param scaling_interval: Time between scaling checks
        """
        # Parse Redis URL
        parsed_url = urllib.parse.urlparse(redis_url)
        redis_host = parsed_url.hostname
        redis_port = parsed_url.port or 6379
        redis_username = parsed_url.username or 'default'
        redis_password = parsed_url.password or ''

        # Redis connection
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            ssl=True if parsed_url.scheme == 'rediss' else False,
            ssl_cert_reqs='none'
        )
        
        # VM Configuration
        self.vm_ips = vm_ips
        self.vm_credentials = vm_credentials
        self.worker_image = worker_image
        self.queue_name = queue_name
        self.jobs_per_worker = jobs_per_worker
        self.scaling_interval = scaling_interval
        
        # Track worker distribution
        self.vm_worker_counts = {vm: 0 for vm in vm_ips}

    def get_ssh_client(self, vm_ip: str) -> paramiko.SSHClient:
        try:
            creds = self.vm_credentials[vm_ip]
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Add timeout to connection attempt
            ssh.connect(
                vm_ip, 
                username=creds['username'], 
                password=creds['password'],
                timeout=10  # 10-second timeout
            )
            return ssh
        except paramiko.AuthenticationException:
            logger.error(f"Authentication failed for {vm_ip}")
            raise
        except paramiko.SSHException as ssh_exception:
            logger.error(f"SSH connection failed to {vm_ip}: {ssh_exception}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to {vm_ip}: {e}")
            raise

    def get_queue_length(self) -> int:
        """Get current queue length from Redis"""
        return self.redis_client.llen(self.queue_name)

    def get_least_loaded_vm(self) -> str:
        """Find VM with least number of workers"""
        return min(self.vm_worker_counts, key=self.vm_worker_counts.get)

    def get_most_loaded_vm(self) -> str:
        """Find VM with most number of workers"""
        return max(self.vm_worker_counts, key=self.vm_worker_counts.get)

    def scale_up_worker(self, vm_ip: str):
        """Scale up workers on a specific VM"""
        try:
            ssh = self.get_ssh_client(vm_ip)
            worker_name = f"worker-{vm_ip.replace('.', '-')}-{int(time.time())}"
            
            # Docker run command with Redis connection
            docker_cmd = (
                f"docker run -d --name {worker_name} "
                f"{self.worker_image}"
            )
            
            # Use get_pty=True to allow sudo execution
            stdin, stdout, stderr = ssh.exec_command(docker_cmd, get_pty=True)
            
            # Send sudo password
            creds = self.vm_credentials[vm_ip]
            stdin.write(creds['password'] + '\n')
            stdin.flush()
            
            # Check for errors
            err = stderr.read().decode()
            out = stdout.read().decode()
            
            if err:
                logger.error(f"Error scaling up on {vm_ip}: {err}")
                return False
            
            # Update tracking
            self.vm_worker_counts[vm_ip] += 1
            logger.info(f"Scaled up worker {worker_name} on {vm_ip}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to scale up on {vm_ip}: {e}")
            return False
        finally:
            ssh.close()

    def scale_down_worker(self, vm_ip: str):
        """
        Scale down workers on a specific VM, preferring containers with lowest CPU usage
        """
        try:
            ssh = self.get_ssh_client(vm_ip)
            creds = self.vm_credentials[vm_ip]
            
            # Retrieve container ID with lowest CPU usage for worker containers
            stats_cmd = (
                " docker ps | grep 'worker-' | awk '{print $1}' | "
                "xargs -I {} sh -c 'echo -n \"{} \"; docker stats --no-stream --format \"{{.CPUPerc}}\" {}' | "
                "sort -k2 -n | head -n 1 | cut -d' ' -f1"
            )
            
            stdin, stdout, stderr = ssh.exec_command(stats_cmd, get_pty=True)
            container_id = stdout.read().decode().strip()
            
            if not container_id:
                logger.warning(f"No workers to remove on {vm_ip}")
                return False
            
            # Remove worker container using container ID
            remove_cmd = f" docker rm -f {container_id}"
            
            # Execute remove command
            stdin, stdout, stderr = ssh.exec_command(remove_cmd, get_pty=True)
            
            # Read and log any output or errors
            err = stderr.read().decode().strip()
            out = stdout.read().decode().strip()
            
            if err:
                logger.error(f"Error scaling down on {vm_ip}: {err}")
                return False
            
            # Verify container removal
            verify_cmd = f" docker ps -a -f id={container_id}"
            stdin, stdout, stderr = ssh.exec_command(verify_cmd, get_pty=True)
            verify_output = stdout.read().decode().strip()
            
            # Log results
            logger.info(f"Removed container {container_id}")
            logger.info(f"Remove command output: {out}")
            logger.info(f"Verification output: {verify_output}")
            
            # Update tracking
            self.vm_worker_counts[vm_ip] -= 1
            logger.info(f"Scaled down worker container {container_id} with lowest CPU usage on {vm_ip}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to scale down on {vm_ip}: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            ssh.close()

    def monitor_and_scale(self):
        """Continuously monitor and scale workers"""
        while True:
            try:
                queue_length = self.get_queue_length()
                total_workers = sum(self.vm_worker_counts.values())
                required_workers = max(1, queue_length // self.jobs_per_worker)

                if required_workers > total_workers:
                    # Scale Up
                    vm_to_scale = self.get_least_loaded_vm()
                    self.scale_up_worker(vm_to_scale)
                
                elif required_workers < total_workers:
                    # Scale Down
                    vm_to_scale = self.get_most_loaded_vm()
                    self.scale_down_worker(vm_to_scale)
                
                time.sleep(self.scaling_interval)
            
            except Exception as e:
                logger.error(f"Scaling error: {e}")
                time.sleep(self.scaling_interval)

def load_config_from_env():
    """
    Load configuration from environment variables
    
    Environment Variables:
    - REDIS_URL: Full Redis connection URL
    - VM_IPS: Comma-separated list of VM IP addresses
    - VM_CREDENTIALS: JSON string of VM credentials
    - WORKER_IMAGE: Docker image for workers (optional)
    - QUEUE_NAME: Redis queue name (optional)
    - JOBS_PER_WORKER: Number of jobs per worker (optional)
    - SCALING_INTERVAL: Time between scaling checks (optional)
    """
    # Redis URL (required)
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        raise ValueError("REDIS_URL environment variable is required")

    # VM IPs (required)
    vm_ips_str = os.environ.get('VM_IPS')
    if not vm_ips_str:
        raise ValueError("VM_IPS environment variable is required")
    vm_ips = vm_ips_str.split(',')

    # VM Credentials (required)
    vm_credentials_str = os.environ.get('VM_CREDENTIALS')
    if not vm_credentials_str:
        raise ValueError("VM_CREDENTIALS environment variable is required")
    
    try:
        vm_credentials = json.loads(vm_credentials_str)
    except json.JSONDecodeError:
        raise ValueError("Invalid VM_CREDENTIALS JSON format")

    # Optional configurations with defaults
    worker_image = os.environ.get('WORKER_IMAGE', 'kathan07/worker')
    queue_name = os.environ.get('QUEUE_NAME', 'code_execution_queue')
    jobs_per_worker = int(os.environ.get('JOBS_PER_WORKER', '5'))
    scaling_interval = int(os.environ.get('SCALING_INTERVAL', '10'))

    return {
        'redis_url': redis_url,
        'vm_ips': vm_ips,
        'vm_credentials': vm_credentials,
        'worker_image': worker_image,
        'queue_name': queue_name,
        'jobs_per_worker': jobs_per_worker,
        'scaling_interval': scaling_interval
    }

def main():
    try:
        # Load configuration from environment variables
        config = load_config_from_env()

        # Create scaling manager with loaded configuration
        manager = WorkerScalingManager(**config)

        # Start monitoring and scaling
        manager.monitor_and_scale()

    except Exception as e:
        logger.error(f"Initialization error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()