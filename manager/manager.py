import os
import time
import redis
import paramiko
import docker
import json
import logging
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkerScalingManager:
    def __init__(self, 
                 redis_url: str, 
                 vm_ips: List[str], 
                 vm_credentials: Dict[str, Dict[str, str]],
                 worker_image: str = 'worker-image:latest',
                 queue_name: str = 'code_execution_queue',
                 jobs_per_worker: int = 5):
        """
        Initialize scaling manager
        
        :param redis_url: External Redis URL
        :param vm_ips: List of VM IP addresses
        :param vm_credentials: SSH credentials for each VM
        :param worker_image: Docker image for workers
        :param queue_name: Redis queue to monitor
        :param jobs_per_worker: Number of jobs per worker before scaling
        """
        # Redis connection
        self.redis_client = redis.from_url(redis_url)
        
        # VM Configuration
        self.vm_ips = vm_ips
        self.vm_credentials = vm_credentials
        self.worker_image = worker_image
        self.queue_name = queue_name
        self.jobs_per_worker = jobs_per_worker
        
        # Track worker distribution
        self.vm_worker_counts = {vm: 0 for vm in vm_ips}

    def get_ssh_client(self, vm_ip: str) -> paramiko.SSHClient:
        """Create SSH client for a VM"""
        creds = self.vm_credentials[vm_ip]
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(vm_ip, username=creds['username'], password=creds['password'])
        return ssh

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
                f"-e REDIS_URL={os.environ.get('REDIS_URL')} "
                f"{self.worker_image}"
            )
            
            stdin, stdout, stderr = ssh.exec_command(docker_cmd)
            
            # Check for errors
            err = stderr.read().decode()
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
            
            # Use docker stats to get CPU usage for worker containers
            stats_cmd = (
                "docker stats --no-stream --format '{{.Name}},{{.CPUPerc}}' "
                "| grep '^worker-' "
                "| sort -t',' -k2 -n "  # Sort by CPU percentage
                "| head -n 1 "  # Get the container with lowest CPU usage
                "| cut -d',' -f1"  # Extract container name
            )
            
            stdin, stdout, stderr = ssh.exec_command(stats_cmd)
            worker_name = stdout.read().decode().strip()
            
            if not worker_name:
                logger.warning(f"No workers to remove on {vm_ip}")
                return False
            
            # Remove worker with lowest CPU usage
            remove_cmd = f"docker stop {worker_name} && docker rm {worker_name}"
            stdin, stdout, stderr = ssh.exec_command(remove_cmd)
            
            # Check for errors
            err = stderr.read().decode()
            if err:
                logger.error(f"Error scaling down on {vm_ip}: {err}")
                return False
            
            # Update tracking
            self.vm_worker_counts[vm_ip] -= 1
            logger.info(f"Scaled down worker {worker_name} with lowest CPU usage on {vm_ip}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to scale down on {vm_ip}: {e}")
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
                
                time.sleep(30)  # Check every 30 seconds
            
            except Exception as e:
                logger.error(f"Scaling error: {e}")
                time.sleep(30)

def main():
    # Configuration from environment variables
    redis_url = os.environ.get('REDIS_URL', 'redis://default:password@host:port')
    vm_ips = os.environ.get('VM_IPS', '192.168.1.100,192.168.1.101,192.168.1.102').split(',')
    
    # VM Credentials (ideally use secure vault/secret management)
    vm_credentials = {
        vm_ip: {
            'username': os.environ.get(f'{vm_ip.replace(".", "_")}_USERNAME'),
            'password': os.environ.get(f'{vm_ip.replace(".", "_")}_PASSWORD')
        } for vm_ip in vm_ips
    }

    manager = WorkerScalingManager(
        redis_url=redis_url,
        vm_ips=vm_ips,
        vm_credentials=vm_credentials
    )

    manager.monitor_and_scale()

if __name__ == "__main__":
    main()