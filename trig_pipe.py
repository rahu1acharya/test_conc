import subprocess

def trigger_concourse_pipeline(target, pipeline, job):
    try:
        result = subprocess.run(
            ['fly', '-t', target, 'trigger-job', '-j', f'{pipeline}/{job}', '--watch'],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("Pipeline triggered successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Failed to trigger the pipeline.")
        print(e.stderr)

if __name__ == "__main__":
    target = 'targetkgvc1'       # Name of your Concourse target
    pipeline = 'pipeline'   # Name of your Concourse pipeline
    job = 'fetch-secrets'             # Name of the job you want to trigger

    trigger_concourse_pipeline(target, pipeline, job)
