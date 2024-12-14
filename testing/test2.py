import asyncio
import aiohttp
import os

async def submit_solution():
    """
    Submit a solution asynchronously to the API
    """
    # Hardcoded file paths
    code_path = './solution.py'
    input_path = './input.txt'
    output_path = './output.txt'

    # Hardcoded language and problem_id as query parameters
    language = 'python'
    problem_id = '541345'

    # Prepare multipart form data
    data = aiohttp.FormData()
    
    # Add files to the body
    data.add_field('code', 
        open(code_path, 'rb'), 
        filename=os.path.basename(code_path)
    )
    data.add_field('input_file', 
        open(input_path, 'rb'), 
        filename=os.path.basename(input_path)
    )
    data.add_field('output_file', 
        open(output_path, 'rb'), 
        filename=os.path.basename(output_path)
    )

    try:
        async with aiohttp.ClientSession() as session:
            # Construct URL with query parameters
            url = f'http://10.20.24.90:8000/submit-solution?language={language}&problem_id={problem_id}'
            
            async with session.post(url, data=data) as response:
                result = await response.json()
                print(f"Submitted job {result['job_id']} - Status: {result['status']}")
                return result
    except Exception as e:
        print(f"Error submitting solution: {e}")
        return None

async def run_load_test(total_requests=100, requests_per_second=10):
    """
    Run load test with concurrent requests
    """
    while True:
        # Create batch of 10 requests
        start_time = asyncio.get_event_loop().time()
        
        # Create tasks for 10 requests
        tasks = [
            submit_solution() for _ in range(requests_per_second)
        ]
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        
        # Calculate time taken and sleep if needed to maintain rate
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        
        if duration < 1:
            await asyncio.sleep(1 - duration)

async def main():
    print("Starting API Load Test...")
    try:
        await run_load_test()
    except KeyboardInterrupt:
        print("\nLoad test stopped by user.")

if __name__ == "__main__":
    asyncio.run(main())