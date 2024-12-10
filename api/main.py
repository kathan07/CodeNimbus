from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List
import redis
import uuid
import os
import shutil
from datetime import datetime
import json

class CodeSubmission(BaseModel):
    language: str = Field(..., description="Programming language (python/c/cpp)")
    problem_id: str = Field(..., description="Unique problem identifier")

app = FastAPI(
    title="Code Execution Submission API",
    description="API for submitting code solutions and tracking job status"
)

# Configure Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Ensure upload directories exist
UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")  # Creates the 'uploads' folder in the current working directory
os.makedirs(UPLOAD_DIR, exist_ok=True)
 
def save_uploaded_file(upload_file: UploadFile, directory: str) -> str:
    """
    Save an uploaded file to a specific directory and return its path
    """
    # Generate unique filename
    file_extension = os.path.splitext(upload_file.filename)[1]
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    file_path = os.path.join(directory, unique_filename)
    
    # Save file
    with open(file_path, 'wb') as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    
    return file_path

@app.post("/submit-solution")
async def submit_solution(
    language: str, 
    problem_id: str,
    background_tasks: BackgroundTasks,
    code: UploadFile = File(...), 
    input_file: UploadFile = File(...), 
    output_file: UploadFile = File(...)
):
    """
    Submit a code solution with input and output files
    """
    # Validate language
    if language not in ['python', 'c', 'cpp']:
        raise HTTPException(status_code=400, detail="Unsupported language")
    
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    try:
        # Save uploaded files
        code_path = save_uploaded_file(code, UPLOAD_DIR)
        input_path = save_uploaded_file(input_file, UPLOAD_DIR)
        output_path = save_uploaded_file(output_file, UPLOAD_DIR)
        
        # Prepare job payload
        job_payload = {
            'job_id': job_id,
            'language': language,
            'problem_id': problem_id,
            'code_file': code_path,
            'input_file': input_path,
            'output_file': output_path,
            'submission_time': str(datetime.now())
        }
        
        # Push job to Redis queue
        redis_client.rpush('code_execution_queue', json.dumps(job_payload))
        
        # Set initial job status
        redis_client.hmset(f'job:{job_id}', {
            'status': 'queued',
            'created_at': str(datetime.now())
        })
        
        return {
            'job_id': job_id,
            'status': 'queued',
            'message': 'Solution submitted successfully'
        }
    
    except Exception as e:
        # Clean up any uploaded files in case of error
        for path in [code_path, input_path, output_path]:
            if path and os.path.exists(path):
                os.unlink(path)
        
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/job-status/{job_id}")
async def get_job_status(job_id: str):
    """
    Retrieve the status of a submitted job
    """
    # Check job result in Redis
    result = redis_client.get(f'job_result:{job_id}')
    
    if result:
        # Job has completed
        job_result = json.loads(result)
        return job_result
    
    # Check job status in Redis hash
    job_status = redis_client.hgetall(f'job:{job_id}')
    
    if not job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Convert byte strings to regular strings
    job_status = {k.decode(): v.decode() for k, v in job_status.items()}
    
    return job_status

@app.get("/recent-submissions")
async def get_recent_submissions(limit: int = 10):
    """
    Retrieve recent job submissions
    """
    # This would typically involve more complex tracking in a production system
    recent_jobs = redis_client.keys('job:*')
    
    submissions = []
    for job_key in recent_jobs[-limit:]:
        job_status = redis_client.hgetall(job_key)
        submission = {
            'job_id': job_key.decode().split(':')[1],
            **{k.decode(): v.decode() for k, v in job_status.items()}
        }
        submissions.append(submission)
    
    return submissions

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)