import os
import boto3
import subprocess
import sys

sys.path.append(("\\".join(sys.path[0].split('\\')[:-2])))

from pprint import pprint
from flow_writer import config

profile = os.environ.get("AWS_PROFILE_NAME", config['project-meta']['profile'])
pn_fallback = config['project-meta']['codebuild']['build-name']
project_name = os.environ.get('CODEBUILD_PROJECT_NAME', pn_fallback)

session = boto3.Session(profile_name=profile)

client = session.client('codebuild')

git_current_branch = subprocess.run(
    ['git', 'rev-parse', '--symbolic-full-name', '--abbrev-ref', 'HEAD'],
    stdout=subprocess.PIPE
).stdout.decode("utf-8").strip()

latest_commit_hash = subprocess.run(
    ['git', 'rev-parse', 'HEAD'],
    stdout=subprocess.PIPE
).stdout.decode("utf-8").strip()

response = client.start_build(
    projectName=project_name,
    sourceVersion=latest_commit_hash
)

pprint(response)
print('\n')

print("CodeBuild processed started for branch: '{}'".format(git_current_branch))
