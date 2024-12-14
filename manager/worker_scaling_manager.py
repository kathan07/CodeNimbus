import os
import time
import redis
import paramiko
import docker
import json
import logging
import traceback
import urllib.parse
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('worker_scaling.log')
    ]
)
logger = logging.getLogger(__name__)

class WorkerScalingManager:
    def __init__(self, 
                 redis_url: str, 
                 vm_ips: List[str], 
                 vm_credentials: Dict[str, Dict[str, str]],
                 worker_image: str = 'kathan07/worker',
                 queue_name: str = 'code_execution_queue',
                 jobs_per_worker: int = 5,
                 scale_interval: int = 30):
        """
        Initialize scaling manager
        
        :param redis_url: External Redis URL
        :param vm_ips: List of VM IP addresses
        :param vm_credentials: SSH credentials for each VM
        :param worker_image: Docker image for workers
        :param queue_name: Redis queue to monitor
        :param jobs_per_worker: Number of jobs per worker before scaling
        :param scale_interval: Interval between scaling checks
        """
        # Parse Redis URL
        parsed_url = urllib.parse.urlparse(redis_url)
        redis_host = parsed_url.hostname
        redis_port = parsed_url.port or 6379
        redis_username = parsed_url.username or 'default'
        redis_password = parsed_url.password or ''

        # Redis connection
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                username=redis_username,
                password=redis_password,
                ssl=True if parsed_url.scheme == 'rediss' else False,
                ssl_cert_reqs='none'
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
        
        # VM Configuration
        self.vm_ips = vm_ips
        self.vm_credentials = vm_credentials
        self.worker_image = worker_image
        self.queue_name = queue_name
        self.jobs_per_worker = jobs_per_worker
        self.scale_interval = scale_interval
        
        # Track worker distribution
        self.vm_worker_counts = {vm: 0 for vm in vm_ips}

    def get_ssh_client(self, vm_ip: str) -> paramiko.SSHClient:
        """
        Create and return an SSH client with robust error handling
        """
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
        try:
            return self.redis_client.llen(self.queue_name)
        except Exception as e:
            logger.error(f"Error getting queue length: {e}")
            return 0

    def get_least_loaded_vm(self) -> str:
        """Find VM with least number of workers"""
        return min(self.vm_worker_counts, key=self.vm_worker_counts.get)

    def get_most_loaded_vm(self) -> str:
        """Find VM with most number of workers"""
        return max(self.vm_worker_counts, key=self.vm_worker_counts.get)

    def scale_up_worker(self, vm_ip: str) -> bool:
        """Scale up workers on a specific VM"""
        ssh = None
        try:
            ssh = self.get_ssh_client(vm_ip)
            creds = self.vm_credentials[vm_ip]
            worker_name = f"worker-{vm_ip.replace('.', '-')}-{int(time.time())}"
            
            # Comprehensive docker run command
            docker_cmd = (
                f"sudo docker run -d --name {worker_name}"
                f"{self.worker_image}"
            )
            
            # Execute command
            stdin, stdout, stderr = ssh.exec_command(docker_cmd)
            
            # Read output and error streams
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            
            # Check for errors
            if err:
                logger.error(f"Error scaling up on {vm_ip}: {err}")
                return False
            
            # Update tracking
            self.vm_worker_counts[vm_ip] += 1
            logger.info(f"Scaled up worker {worker_name} on {vm_ip}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to scale up on {vm_ip}: {e}")
            traceback.print_exc()
            return False
        finally:
            if ssh:
                ssh.close()

    def scale_down_worker(self, vm_ip: str) -> bool:
        """
        Scale down workers on a specific VM, preferring containers with lowest CPU usage
        """
        ssh = None
        try:
            ssh = self.get_ssh_client(vm_ip)
            
            # Command to get worker containers with CPU usage
            stats_cmd = (
                "sudo docker ps | grep 'worker-' | awk '{print $1}' | "
                "xargs -I {} sh -c 'docker stats --no-stream --format \"{{.ID}},{{.CPUPerc}}\" {}' | "
                "sort -t',' -k2 -n | head -n 1 | cut -d',' -f1"
            )
            
            # Execute the command directly
            stdin, stdout, stderr = ssh.exec_command(stats_cmd)
            
            # Read container ID
            container_id = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            
            if err:
                logger.error(f"Error finding container to remove on {vm_ip}: {err}")
                return False
            
            if not container_id:
                logger.warning(f"No workers to remove on {vm_ip}")
                return False
            
            # Remove worker container
            remove_cmd = f"docker rm -f {container_id}"
            
            # Execute remove command
            stdin, stdout, stderr = ssh.exec_command(remove_cmd)
            
            # Read output and errors
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            
            if err:
                logger.error(f"Error removing container on {vm_ip}: {err}")
                return False
            
            # Log results
            logger.info(f"Removed container {container_id}")
            logger.info(f"Remove command output: {out}")
            
            # Update tracking
            self.vm_worker_counts[vm_ip] -= 1
            logger.info(f"Scaled down worker container {container_id} with lowest CPU usage on {vm_ip}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to scale down on {vm_ip}: {e}")
            traceback.print_exc()
            return False
        finally:
            if ssh:
                ssh.close()

    def monitor_and_scale(self):
        """Continuously monitor and scale workers"""
        logger.info("Starting worker scaling monitoring...")
        while True:
            try:
                # Get current queue length and calculate required workers
                queue_length = self.get_queue_length()
                total_workers = sum(self.vm_worker_counts.values())
                required_workers = max(1, queue_length // self.jobs_per_worker)

                logger.info(f"Current metrics - Queue: {queue_length}, Total Workers: {total_workers}, Required Workers: {required_workers}")

                # Scaling logic
                if required_workers > total_workers:
                    # Scale Up
                    vm_to_scale = self.get_least_loaded_vm()
                    logger.info(f"Scaling up on VM {vm_to_scale}")
                    self.scale_up_worker(vm_to_scale)
                
                elif required_workers < total_workers:
                    # Scale Down
                    vm_to_scale = self.get_most_loaded_vm()
                    logger.info(f"Scaling down on VM {vm_to_scale}")
                    self.scale_down_worker(vm_to_scale)
                
                # Wait before next check
                time.sleep(self.scale_interval)
            
            except Exception as e:
                logger.error(f"Scaling cycle error: {e}")
                traceback.print_exc()
                time.sleep(self.scale_interval)


def main():
    # Configuration from environment variables with fallback defaults
    redis_url = os.environ.get(
        'REDIS_URL', 
        'rediss://default:password@your-redis-host:6379'
    )
    
    # VM IPs from environment or default
    vm_ips = '192.168.122.7,192.168.122.121'.split(',')
    
    # VM Credentials from environment or default
    # In production, use secure secret management!
    vm_credentials = {
        vm_ip: {
            'username': 'cloud',
            'password': 'cloud'
        } for vm_ip in vm_ips
    }

    # Create scaling manager
    manager = WorkerScalingManager(
        redis_url=redis_url,
        vm_ips=vm_ips,
        vm_credentials=vm_credentials
    )

    # Start monitoring and scaling
    try:
        manager.monitor_and_scale()
    except KeyboardInterrupt:
        logger.info("Worker scaling monitoring stopped.")
    except Exception as e:
        logger.error(f"Unhandled error in main: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()